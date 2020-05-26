import time
import os
import pathlib


class LogHandler:

    @staticmethod
    def log_msg(msg):
        now = time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(int(round(time.time() * 1000)) / 1000))
        filenow = time.strftime('%Y%m%d', time.localtime(int(round(time.time() * 1000)) / 1000))
        log_msg = "[%s] %s\r\n" % (
            now,
            msg
        )
        directory = os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + ".") + "\\log"

        # fp = pathlib.Path().absolute()+"\\"+directory+"\\"
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(
                directory + "\\log_" + filenow + ".log",
                "a", encoding='utf-8') as f:
            f.write(log_msg)
        print(msg)

    @staticmethod
    def log_properties(filename, p):
        now = time.strftime('%Y/%m/%d', time.localtime(int(round(time.time() * 1000)) / 1000))
        directory = os.path.abspath(
            os.path.dirname(os.path.abspath(__file__)) + os.path.sep + ".") + "\\Fields_not_in_definition"
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(
                directory + "\\" + filename + ".log", "a+", encoding='utf-8') as f:
            line_found = any(p in line for line in f)
            if not line_found:
                f.write(p + "\r\n")

    @staticmethod
    def log_exceptions(msg):
        now = time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(int(round(time.time() * 1000)) / 1000))
        filenow = time.strftime('%Y%m%d', time.localtime(int(round(time.time() * 1000)) / 1000))
        log_msg = "[%s] %s\r\n" % (
            now,
            msg
        )
        directory = os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + ".") + "\\log"

        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(
                directory + "\\exceptions_" + filenow + ".log",
                "a", encoding='utf-8') as f:
            f.write(log_msg)
        print(msg)