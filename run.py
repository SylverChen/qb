import resource
import logging
import os
import multiprocessing
import pexpect
import psutil
import time
import numpy as np
import subprocess

DIR_LOG = '/home/sylver/Projects/env/queryperformance20170717/log/'
DIR_NAME = '/home/sylver/Projects/env/queryperformance20170717/v2.4.0/query/'
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


def worker(_id, _type, user_id):
    logger = init_logger(str(_id)+'_'+_type+'_'+str(user_id))
    p = multiprocessing.current_process()
    scripts = [name for name in os.listdir(DIR_NAME+_type+'/'+str(user_id))]
    child = pexpect.spawn('psql --username=user' +
                          str(user_id)+' --dbname=tpcdsgb',
                          maxread=1000000)
    for (script_id, script) in enumerate(scripts):
        print('----%s running by User %d now!-----' % (script, user_id))
        logger.info('[%d]:[%s]:[%d]:[%s]:[%d]' %
                    (script_id, script, _id, _type, user_id))

        try:
            cpu_util = psutil.cpu_times_percent()
            mem_util = psutil.virtual_memory()
            logger.info(cpu_util)
            logger.info(mem_util)
            usage = resource.getrusage(resource.RUSAGE_SELF)
            for name, desc in RESOURCES:
                logger.info('{:<25} ({:<10}) = {}'.format(
                    desc, name, getattr(usage, name)))
            index = child.expect(['tpcdsgb=#', 'END', ':'], timeout=30)
            if index == 0:
                child.sendline('q;')
            elif index == 1:
                child.sendline('q')
            elif index == 2:
                child.sendline('q')
            print(child.before)
            print(DIR_NAME + _type + script)
            child.expect('tpcdsgb=#')
            child.sendline('\i ' + DIR_NAME + _type + '/' +
                           str(user_id) + '/' + script)

        except pexpect.TIMEOUT as e:
            logger.error('failed with %s\n' % str(child))

    usage = resource.getrusage(resource.RUSAGE_SELF)
    for name, desc in RESOURCES:
        logger.info('{:<25} ({:<10}) = {}'.format(
            desc, name, getattr(usage, name)))

    index = child.expect(['tpcdsgb=#', 'END', ':'], timeout=30)
    if index == 0:
        child.sendline('q;')
    elif index == 1:
        child.sendline('q')
    elif index == 2:
        child.sendline('q')

    child.expect('tpcdsgb=#')
    child.sendline('\q')
    child.close()
    logger.info('-----Process [%d]:[%s] finished-----\n' % (p.pid, p.name))
    print('-----Process %s finished-----\n' % p.name)


def start_works(_id, _type, row):
    start = sum(row[:_id])
    end = sum(row[:_id+1])

    for user_id in range(start, end):
        p = multiprocessing.Process(
            target=worker,
            args=(_id, _type, user_id),
            # args=(_type, logger,),
            name=user_id)
        p.daemon = False
        # jobs.append(p)
        p.start()


if __name__ == "__main__":
    subprocess.run(['rm', '-r', 'log'])
    subprocess.run(['rm', '-r', 'explain'])
    subprocess.run(['rm', '-r', 'res'])
    subprocess.run(['mkdir', 'log'])
    subprocess.run(['mkdir', 'explain'])
    subprocess.run(['mkdir', 'res'])

    samples = np.loadtxt('lhs', dtype='int')
    row = samples[0]
    types = [name for name in os.listdir(DIR_NAME)
             if (os.path.isdir(os.path.join(DIR_NAME, name))
                 and name.startswith('type_'))]
    types.sort()

    # jobs = []

    for (_id, pair) in enumerate(zip(types, row)):
        start_works(_id, pair[0], row)

    # time.sleep(120)
