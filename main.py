import os
from locale import *
from threading import Thread
from StreamThread import StreamThread
import time
from logHandler import LogHandler

thread_num = 4

file_nums = 0
threaded_file = []
for root, directories, files in os.walk("csv"):
    all_files = len(files)
    sublist_size = round(len(files) / thread_num)
    threaded_file = [files[i:i + sublist_size] for i in range(0, len(files), sublist_size)]
    start_time = time.process_time()
    threads = []
    for index, tf in enumerate(threaded_file):
        threads.append(Thread(target=StreamThread().streaming, args=(tf, "Thread-{0}".format(index))))
        try:
            threads[index].start()
        except Error as err:
            print("Error: unable to start thread, msg: {0}".format(err))
        finally:
            end_time = time.process_time()
            LogHandler.log_msg(
                "Main thread performance average / file: %ss" % round((start_time - end_time) / len(files), 3))
            pass




