#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
from multiprocessing import Pool
from threading import Thread
from queue import Queue, Empty
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache
import time

END_OF_DATA = "END"
NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])
DATA_PART_APPEND_SIZE = 1000
MAX_TRIES = 3
TIMEOUT = 0.5


class ToMemcache(Thread):
    """
    Класс - потомок класса Thread с переопределенным
    методом run для вставки данных в мемкещ
    params: -мемкеш
            -очередь для части данных
            -очередь для ошибок и профитов
            -опция dry
    """
    def __init__(self, memc, data_queue, result_queue, dry):
        super().__init__()
        self.data_queue = data_queue
        self.result_queue = result_queue
        self.memc = memc
        self.dry = dry

    def run(self):
        logging.info('[Worker %s] started: %s' % (os.getpid(), self.name))
        processed = errors = 0
        while True:
            # Получаем данные из очереди
            data_part = self.data_queue.get(block=True, timeout=None)

            # Если видим флаг конца данных - прерываем
            # бесконечный цикл вставки данных в мемкеш
            if data_part == END_OF_DATA:
                self.result_queue.put((processed, errors))
                break
            else:
                # Вставляем данные в мемкеш (подсчитываем ошибки и профиты)
                # Сериализация данных
                data_to_paste = {}
                for appsinstalled in data_part:
                    ua = appsinstalled_pb2.UserApps()
                    ua.lat = appsinstalled.lat
                    ua.lon = appsinstalled.lon
                    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
                    ua.apps.extend(appsinstalled.apps)
                    packed = ua.SerializeToString()
                    data_to_paste[key] = packed
                processed, errors = self.insert_appsinstalled(data_to_paste)


    def insert_appsinstalled(self, data_to_paste):
        """
        Метод сериализации данных и отправки их в мемкеш
        """
        processed = errors = 0
        # Вставка данных
        try:
            if self.dry:
                for key, value in data_to_paste.items():
                    logging.debug("%s - %s -> %s" % (self.memc, key, str(value).replace("\n", " ")))
                    processed += 1
            else:
                # вызываем метод вставки данных в мемкеш
                 processed, errors = self.memc_set(data_to_paste)
        except Exception as e:
            logging.exception("Cannot write to memc %s: %s" % (self.memc, e))
            return False
        return processed, errors


    def memc_set(self, data_to_paste):
        """
        метод вставки данных в мемкеш с
        """
        trying = 0
        failed = self.memc.set_multi(data_to_paste)
        while failed and trying < MAX_TRIES:
            failed = self.memc.set_multi({
                key: data_to_paste[key]
                for key in failed
            })
            time.sleep(0.5)
            trying += 1          
        return len(data_to_paste.keys()) - len(failed), len(failed)


def dot_rename(path):
    head, fn = os.path.split(path)
    os.rename(path, os.path.join(head, "." + fn))


def parse_appsinstalled(line):
    """
    Функция парсинга одной строки данных лога
    для получения интересующих нас данных и создания объекта AppsInstalled
    """
    line_parts = line.strip().split("\t")
    if (len(line_parts) in range 0, 5):
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
        
    apps = [int(a.strip()) for a in raw_apps.split(",")]
        
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def parse_files(fn_opts):
    """
    Функция парсинга фалов
    Создает потоки для вставки данных в мемкеш по конкретным
    идентификаторам девайсов
    """
    fn, options = fn_opts
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    processed = errors = 0
    logging.info('Processing %s' % fn)
    # словарь для хранения очередей
    insert_data = {}
    # словарь для хранения воркеров
    threads = {}
    # Очередь для хранения информации об
    # успешно вставленных данных и об ошибках
    processed_errors_queue = Queue()
    for platform, memc_addr in device_memc.items():
        # создание отдельного мемкэша для конкретных
        # идентификаторов девайсов
        memc = memcache.Client([memc_addr])
        # создание отдельных очередей для
        # хранения части спарсенных данных по
        # идентификаторам девайсов для вставки их
        # в мемкеш (обмен между producer-consumer)
        insert_data[platform] = Queue()
        # создаем воркеры для параллельного залива данных в мемкеш
        worker = ToMemcache(memc, insert_data[platform], processed_errors_queue, options.dry)
        threads[platform] = worker
        # запускаем процессы
        worker.start()

    fd = gzip.open(fn)
    # словарь
    data_part = {}
    # Парсинг данных построчно в файле
    for line in fd:
        line = line.strip()
        if not line:
            continue
        # Парсинг конкртеной строки и разделение данных
        appsinstalled = parse_appsinstalled(line.decode())
        if not appsinstalled:
            errors += 1
            continue
        memc_addr = device_memc.get(appsinstalled.dev_type)
        if not memc_addr:
            errors += 1
            logging.error("Unknow device type: %s" % appsinstalled.dev_type)
            continue
        # создаем или добавляем в словарь данные для передачи в очередь кусками
        if appsinstalled.dev_type not in data_part.keys():
            data_part[appsinstalled.dev_type] = []
            data_part[appsinstalled.dev_type].append(appsinstalled)
        else:
            data_part[appsinstalled.dev_type].append(appsinstalled)
        # если значение длины словаря больше чем нам необходимо, 
        # то передаем данные в нужную очередь (по dev_type разделение)
        # и очищаем список по ключу словаря для след партии данных 
        if len(data_part[appsinstalled.dev_type]) > DATA_PART_APPEND_SIZE:
            insert_data[appsinstalled.dev_type].put(data_part[appsinstalled.dev_type])
            data_part[appsinstalled.dev_type] = []
    # если данные в спарсенном файле закончились, то добавляем в
    # очередь флаг окончания данные, чтобы выйти из бесконечного
    # цикла в методе, который вставляет данные в мемкеш
    for platform in device_memc.keys():
        insert_data[platform].put(END_OF_DATA)
    # Вызываем метод join() для каждого потока
    for platform in device_memc.keys():
        threads[platform].join()
    # Ждем пока очередь, в которой количество ошибок и успешно вставленных
    # данных не будет пуста (закончится вставка всех данных)
    # Суммируем processed и errors
    while not processed_errors_queue.empty():
        result = processed_errors_queue.get(timeout=0.1)
        processed += result[0]
        errors += result[1]
    # Подсчитываем соотношение успешно вставленных данных и ошибок
    err_rate = float(errors) / processed
    if err_rate < NORMAL_ERR_RATE:
        logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
    else:
        logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
    fd.close()
    return fn


def main(options):
    """
    Основная функция запуска парсера логов
    Создает указанное пользователем кол-во процессов
    Каждый процесс отвечает за парсинг файла
    в указанной диркектории
    """
    pool = Pool(int(options.workers))
    for return_file_name in pool.map(parse_files, [(file_name, options) for file_name in glob.iglob(options.pattern)]):
        dot_rename(return_file_name)


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("-w", "--workers", action="store", default=3)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)
    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except KeyboardInterrupt:
            # Ctrl-C handling and send kill to threads
            print "Sending kill to threads..."
            for t in threads:
                t.kill_received = True
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
