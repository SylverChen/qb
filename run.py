
import resource
import logging
import os
import multiprocessing
import pexpect
import psutil
import time
import numpy as np
import subprocess
import random

DIR = '/home/sylver/Projects/env/queryperformance20170717/'
DIR_LOG = DIR + 'log/'
DIR_NAME = DIR + 'v2.4.0/query/tpcdsfive/'
# DIR_NAME = DIR + 'v2.4.0/query/'
INPUT_FILE = DIR + 'run_records/running'
RESOURCES = [
    ('ru_utime', 'User time'),
    ('ru_stime', 'System time'),
    ('ru_minflt', 'Page faults not requiring I/O'),
    ('ru_majflt', 'Page faults requiring I/O'),
    ('ru_inblock', 'Block inputs'),
    ('ru_oublock', 'Block outputs'),
]
# pattern = re.compile(r'\((\d)+\s*row')


def init_logger(_type):
    logger = logging.getLogger(_type)
    logger.setLevel(logging.DEBUG)

    handler = logging.FileHandler(os.path.join(DIR_LOG, _type))
    handler.setLevel(logging.DEBUG)

    # ('%Y-%m-%d %H:%M:%S, milliseconds)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger


def worker(_event, group_id, type_id, _type, user_id):
    num = 0
    logger = init_logger(str(group_id) + '_' + str(type_id) + '_' +
                         _type + '_' + str(user_id))
    # p = multiprocessing.current_process()
    scripts = [name for name in os.listdir(DIR_NAME+_type+'/'+str(user_id))]
    random.shuffle(scripts)
    child = pexpect.spawn('psql --username=user' +
                          str(user_id)+' --dbname=tpcdshigh',
                          maxread=1000000000)
    for (script_id, script) in enumerate(scripts):
        num += 1
        if (_event.is_set()):
            break
        # if num >= 30:
        #     break
        print('----%s running number %d by User %d in Group %d now!-----' %
              (script, num, user_id, group_id))

        try:

            index = child.expect(['tpcdshigh=#', 'END', ':'], timeout=1400)
            if index == 0:
                child.sendline('q;')
            elif index == 1:
                child.sendline('q')
            elif index == 2:
                child.sendline('q')
            print(child.before)
            print(DIR_NAME + _type + script)
            child.expect('tpcdshigh=#')

            logger.info('[%d]:[%s]:[%d]:[%d]:[%s]:[%d]' %
                        (script_id, script, group_id, type_id, _type, user_id))

            cpu_util = psutil.cpu_times_percent()
            mem_util = psutil.virtual_memory()
            logger.info(cpu_util)
            logger.info(mem_util)

            usage = resource.getrusage(resource.RUSAGE_SELF)
            for name, desc in RESOURCES:
                logger.info('{:<25} ({:<10}) = {}'.format(
                    desc, name, getattr(usage, name)))

            script_path = os.path.join(DIR_NAME, _type, str(user_id), script)
            child.sendline('\i ' + script_path)

        except pexpect.TIMEOUT as e:
            logger.error('failed with %s\n' % str(child))

    try:
        index = child.expect(['tpcdshigh=#', 'END', ':'], timeout=60)
        if index == 0:
            child.sendline('q;')
        elif index == 1:
            child.sendline('q')
        elif index == 2:
            child.sendline('q')

        child.expect('tpcdshigh=#')
        # child.sendline('\q')
        # child.close()

    except pexpect.TIMEOUT as e:
        logger.error('failed with %s\n' % str(child))

    usage = resource.getrusage(resource.RUSAGE_SELF)
    for name, desc in RESOURCES:
        logger.info('{:<25} ({:<10}) = {}'.format(
            desc, name, getattr(usage, name)))

    logger.info('-----User %d in Group %d finished-----\n' %
                (user_id, group_id))
    print('-----User %d in Group %d finished-----\n' % (user_id, group_id))
    _event.set()


def start_works(e, group_id, type_id, _type, row, jobs):
    start = sum(row[:type_id])
    end = sum(row[:type_id+1])

    for usertype_id in range(start, end):
        p = multiprocessing.Process(
            target=worker,
            args=(e, group_id, type_id, _type, usertype_id),
            # args=(_type, logger,),
            name=usertype_id)
        p.daemon = False
        jobs.append(p)
        p.start()


def main():
    subprocess.run(['rm', '-r', 'log'])
    subprocess.run(['rm', '-r', 'explain'])
    subprocess.run(['rm', '-r', 'res'])
    subprocess.run(['mkdir', 'log'])
    subprocess.run(['mkdir', 'explain'])
    subprocess.run(['mkdir', 'res'])

    e = multiprocessing.Event()
    samples = np.loadtxt('samples_24mpl', dtype='int')
    # samples = np.loadtxt('samples_1gb_24mpl', dtype='int')

    indexes = []
    with open(INPUT_FILE, 'rt') as f:
        for line in f:
            indexes.append(int(line.strip()))
    indexes = indexes[9:12]
    print('-----now we are running-----\n')
    print(indexes)
    # with open(OUTPUT_FILE, 'w') as f:
    #     f.write('\n'.join(str(item) for item in indexes))

    types = [name for name in os.listdir(DIR_NAME)
             if (os.path.isdir(os.path.join(DIR_NAME, name))
                 and name.startswith('type_'))]
    types.sort()

    for group_id in indexes:
        row = samples[group_id]
        e.clear()
        jobs = []
        for (type_id, pair) in enumerate(zip(types, row)):
            start_works(e, group_id, type_id, pair[0], row, jobs)
        for j in jobs:
            j.join()
        print('========== Group %d finished==========\n' % group_id)
        # time.sleep(10)


if __name__ == "__main__":
    main()
