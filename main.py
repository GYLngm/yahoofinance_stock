import os
from locale import *
from threading import Thread
import StreamThread
import time
from logHandler import LogHandler

thread_num = 6

file_nums = 0
threaded_file = []
for root, directories, files in os.walk("csv"):
    all_files = len(files)
    sublist_size = round(len(files) / thread_num)
    threaded_file = [files[i:i + sublist_size] for i in range(0, len(files), sublist_size)]
    start_time = time.process_time_ns()
    threads = []
    for index, tf in enumerate(threaded_file):
        threads.append(Thread(target=StreamThread.streaming, args=(tf, "Thread-{}".format(index))))
        try:
            threads[index].start()
            threads[index].join()
        except Error as err:
            LogHandler.log_exceptions("Error: unable to start thread, msg: {0}".format(err))
        finally:
            end_time = time.process_time_ns()
            LogHandler.log_exceptions(
                "Main thread performance average / file: %ss" % round((end_time - start_time)/len(files)*1000000, 5))
    end_time = time.process_time_ns()
    print("Main time spend: %ss" % round((end_time-start_time)/1000000, 5))


