import os
import re
import subprocess

DIR_CURRENT = '/home/sylver/Projects/env/queryperformance20170717/'
DIR_LOG = DIR_CURRENT + 'explain/'
DIR_RES = DIR_CURRENT + 'res/'


def mv_log():
    pattern = re.compile(r'_(?P<pattern>(\d)+)_')
    logs = [name for name in os.listdir(DIR_LOG)
            if (os.path.isfile(os.path.join(DIR_LOG, name)))]
    logs.sort()

    set_logs = set()
    for log in logs:
        match = re.search(pattern, log)
        set_logs.add(match.group('pattern'))

    for item in set_logs:
        subprocess.run(['mkdir', DIR_LOG + item])

    dir_process = [name for name in os.listdir(DIR_LOG)
                   if (os.path.isdir(os.path.join(DIR_LOG, name)))]
    for _dir in dir_process:
        logs = [name for name in os.listdir(DIR_LOG)
                if (os.path.isfile(os.path.join(DIR_LOG, name)))]
        for log in logs:
            if log.find('_' + _dir + '_') > 0:
                # print(os.path.join(DIR_LOG, log))
                # print(os.path.join(DIR_LOG, _dir))
                subprocess.run(['mv', os.path.join(DIR_LOG, log),
                                os.path.join(DIR_LOG, _dir)])
            else:
                continue


def mv_res():
    pattern = re.compile(r'(?P<pattern>(\d)+_type_(\d)+)')
    reses = [name for name in os.listdir(DIR_RES)
             if (os.path.isfile(os.path.join(DIR_RES, name)))]
    reses.sort()

    set_reses = set()
    for res in reses:
        match = re.search(pattern, res)
        set_reses.add(match.group('pattern'))

    for item in set_reses:
        subprocess.run(['mkdir', DIR_RES + item])

    dir_name = [name for name in os.listdir(DIR_RES)
                if (os.path.isdir(os.path.join(DIR_RES, name)))]

    for _dir in dir_name:
        reses = [name for name in os.listdir(DIR_RES)
                 if (os.path.isfile(os.path.join(DIR_RES, name)))]
        for res in reses:
            if res.find(_dir) > 0:
                # print(os.path.join(DIR_RES, res))
                # print(os.path.join(DIR_RES, _dir))
                subprocess.run(['mv', os.path.join(DIR_RES, res),
                                os.path.join(DIR_RES, _dir)])
            else:
                continue


def main():
    mv_log()
    mv_res()


if __name__ == "__main__":
    main()
