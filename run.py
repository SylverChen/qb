import time
import resource
import logging
import os
import multiprocessing
import pexpect
import psutil


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


def worker(_type):
    logger = init_logger(_type)
    p = multiprocessing.current_process()
    scripts = [name for name in os.listdir(DIR_NAME + _type)]
    child = pexpect.spawn('psql --username=sylver --dbname=tpcdsgb')
    for (_id, script) in enumerate(scripts):
        print('----%s running in Process %s now!-----' % (script, p.name))
        logger.info('[%d]:[%s]:[%d]:[%s]' % (_id, script, p.pid, p.name))

        try:

            cpu_util = psutil.cpu_times_percent()
            mem_util = psutil.virtual_memory()
            logger.info(cpu_util)
            logger.info(mem_util)
            usage = resource.getrusage(resource.RUSAGE_SELF)
            for name, desc in RESOURCES:
                logger.info('{:<25} ({:<10}) = {}'.format(
                    desc, name, getattr(usage, name)))
            child.expect('tpcdsgb=#', timeout=240)
            print(child.before)
            print(DIR_NAME + _type + script)
            child.sendline('\i ' + DIR_NAME + _type + '/' + script)

        except pexpect.TIMEOUT as e:
            logger.error('failed with %s\n' % str(child))

    usage = resource.getrusage(resource.RUSAGE_SELF)
    for name, desc in RESOURCES:
        logger.info('{:<25} ({:<10}) = {}'.format(
            desc, name, getattr(usage, name)))

    child.expect('tpcdsgb=#')
    child.sendline('\q')
    child.close()
    print('-----Process %s finished-----\n' % p.name)


if __name__ == "__main__":
    types = [name for name in os.listdir(DIR_NAME)
             if (os.path.isdir(os.path.join(DIR_NAME, name))
                 and name.startswith('type_'))]
    types.sort()
    jobs = []
    for _type in types:
        p = multiprocessing.Process(
            target=worker,
            args=(_type,),
            # args=(_type, logger,),
            name=_type)
        p.daemon = False
        jobs.append(p)
        p.start()

    time.sleep(240)
