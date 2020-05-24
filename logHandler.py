import time
import os


class LogHandler:

    @staticmethod
    def log_msg(msg):
        now = time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(int(round(time.time() * 1000)) / 1000))
        filenow = time.strftime('%Y%m%d', time.localtime(int(round(time.time() * 1000)) / 1000))
        log_msg = "[%s] %s" % (
            now,
            msg
        )
        directory = "log"
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open("./%s/%s" % (
                directory,
                "log_" + filenow + ".log"
        ), "a+", encoding='utf-8') as f:
            f.write(log_msg)
        print(msg)

    @staticmethod
    def log_properties(filename, p):
        now = time.strftime('%Y/%m/%d', time.localtime(int(round(time.time() * 1000)) / 1000))
        directory = "./Fields_not_in_definition/"
        if not os.path.exists(directory):
            os.makedirs(directory)
        directory += filename + ".log"

        with open(directory, "w+", encoding='utf-8') as f:
            line_found = any(p in line for line in f)
            if not line_found:
                f.write(p + "\n")
