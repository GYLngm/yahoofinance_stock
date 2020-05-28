import os
from locale import *
from threading import Thread
from StreamThread import StreamThread
import time
from logHandler import LogHandler

thread_num = 4

file_nums = 0
threaded_file = []

if __name__ == '__main__':
    start_time = time.process_time_ns()
    for root, directories, files in os.walk("csv"):
        all_files = len(files)
        sublist_size = round(len(files) / thread_num)
        threaded_file = [files[i:i + sublist_size] for i in range(0, len(files), sublist_size)]
        threads = []
        for index, tf in enumerate(threaded_file):
            p = StreamThread(sublist=tf, threadCode="Thread-{}".format(index))
            threads.append(p)

        for th in threads:
            try:
                th.start()
                th.join()
            except Error as err:
                LogHandler.log_exceptions("Error: unable to start thread, msg: {0}".format(err))

    end_time = time.process_time_ns()
    print("All threads time spend: %sms" % round((end_time-start_time)/1000000, 5))


