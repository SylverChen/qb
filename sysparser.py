import mmap
import re
import os

DIR_LOG = './log/'

# match this line:
# 2017-07-26 15:24:02,862 - 0_0_type_13_0 - INFO - [0]:[query_0.sql]:[0]:[0]:[type_13]:[0]
process = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2}(\,(\d)+)?)
    .*\[
    (?P<_id>(\d)+)
    \]:\[
    (?P<script_name>.*sql)
    \]:\[
    (?P<group_id>(\d)+)
    \]:\[
    (?P<type_id>(\d)+)
    \]:\[
    (?P<type_name>type_(\d)+)
    \]:\[
    (?P<user_id>(\d)+)
    ''',
    re.VERBOSE)

cpu_util = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2}(\,(\d)+)?)
    .*scputimes
    .*user=
    (?P<user>(\d)+(\.(\d)+)?)
    .*system=
    (?P<system>(\d)+(\.(\d)+)?)
    .*idle=
    (?P<idle>(\d)+(\.(\d)+)?)
    .*iowait=
    (?P<iowait>(\d)+(\.(\d)+)?)
    .*irq=
    (?P<irq>(\d)+(\.(\d)+)?)
    .*softirq=
    (?P<softirq>(\d)+(\.(\d)+)?)
    ''',
    re.VERBOSE)

mem_util = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2}(\,(\d)+)?)
    .*svmem
    .*used=
    (?P<used>(\d)+(\.(\d)+)?)
    .*free=
    (?P<free>(\d)+(\.(\d)+)?)
    .*buffers=
    (?P<buffers>(\d)+(\.(\d)+)?)
    .*cached=
    (?P<cached>(\d)+(\.(\d)+)?)
    ''',
    re.VERBOSE)

utime = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2})
    .*ru_utime
    .*
    =
    \s*
    (?P<utime>(\d)+(\.(\d)+)?)
    ''',
    re.VERBOSE)
stime = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2})
    .*ru_stime
    .*
    =
    \s*
    (?P<stime>(\d)+(\.(\d)+)?)
    ''',
    re.VERBOSE)
minflt = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2})
    .*ru_minflt
    .*
    =
    \s*
    (?P<minflt>(\d)+(\.(\d)+)?)
    ''',
    re.VERBOSE)
majflt = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2})
    .*ru_majflt
    .*
    =
    \s*
    (?P<majflt>(\d)+(\.(\d)+)?)
    ''',
    re.VERBOSE)
inblock = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2})
    .*ru_inblock
    .*
    =
    \s*
    (?P<inblock>(\d)+(\.(\d)+)?)
    ''',
    re.VERBOSE)
oublock = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2})
    .*ru_oublock
    .*
    =
    \s*
    (?P<oublock>(\d)+(\.(\d)+)?)
    ''',
    re.VERBOSE)

def log_single(list_utime, list_stime, list_minflt, list_majflt,
               list_inblock, list_oublock, mm, match, _id):
# def log_single(list_res, mm, match, _id):
    _number = match.group('_id').decode()
    group_id = match.group('group_id').decode()
    type_id = match.group('type_id').decode()
    user_id = match.group('user_id').decode()
    script_name = match.group('script_name').decode()
    # process_id = match.group('process_id').decode()
    # _type = match.group('type').decode()

    res_name = str(_id) + '_' + str(group_id) + '_' + str(type_id) + \
        '_' + str(user_id) + '_' + script_name[:-4]

    with open('./res/'+res_name, 'wt') as resf:
        resf.write('{\n')
        resf.write('"' + '_id' + '": ' + '"' + str(_number) + '",\n')
        resf.write('"' + 'type_id' + '": ' + '"' + type_id + '",\n')
        resf.write('"' + 'user_id' + '": ' + '"' + user_id + '",\n')
        resf.write('"' + 'script_name' + '": ' + '"' + script_name + '",\n')
        # resf.write('"' + 'Timestamp' + '": ' + '"' + timestamp + '",\n')
        # resf.write('"' + 'Process Id' + '": ' + '"' + process_id + '",\n')
        # resf.write('"' + 'Script Name' + '": ' + '"' + script + '",\n')
        # resf.write('"' + 'Query Type' + '": ' + '"' + _type + '",\n')

        cpu_util_match = re.search(cpu_util, mm)
        cpu_user = cpu_util_match.group('user').decode()
        cpu_system = cpu_util_match.group('system').decode()
        cpu_idle = cpu_util_match.group('idle').decode()
        cpu_iowait = cpu_util_match.group('iowait').decode()
        cpu_irq = cpu_util_match.group('irq').decode()
        cpu_softirq = cpu_util_match.group('softirq').decode()
        resf.write('"' + 'cpu user' + '": ' + '"' + cpu_user + '",\n')
        resf.write('"' + 'cpu system' + '": ' + '"' + cpu_system + '",\n')
        resf.write('"' + 'cpu idle' + '": ' + '"' + cpu_idle + '",\n')
        resf.write('"' + 'cpu iowait' + '": ' + '"' + cpu_iowait + '",\n')
        resf.write('"' + 'cpu irq' + '": ' + '"' + cpu_irq + '",\n')
        resf.write('"' + 'cpu softirq' + '": ' + '"' + cpu_softirq + '",\n')

        mem_util_match = re.search(mem_util, mm)
        mem_used = mem_util_match.group('used').decode()
        mem_free = mem_util_match.group('free').decode()
        mem_buffers = mem_util_match.group('buffers').decode()
        mem_cached = mem_util_match.group('cached').decode()
        resf.write('"' + 'mem used' + '": ' + '"' + mem_used + '",\n')
        resf.write('"' + 'mem free' + '": ' + '"' + mem_free + '",\n')
        resf.write('"' + 'mem buffers' + '": ' + '"' + mem_buffers + '",\n')
        resf.write('"' + 'mem cached' + '": ' + '"' + mem_cached + '",\n')

        # utime_match = re.search(utime, mm)
        # resource_utime = utime_match.group('utime').decode()
        print(_id)
        resource_utime = float(list_utime[_id+1]) - float(list_utime[_id])
        # resource_utime = float(list_res[_id+1][0]) - float(list_res[_id][0])
        resf.write('"' + 'resource utime' + '": ' +
                   '"' + str(resource_utime) + '",\n')

        # stime_match = re.search(stime, mm)
        # resource_stime = stime_match.group('stime').decode()
        resource_stime = float(list_stime[_id+1]) - float(list_stime[_id])
        # resource_stime = float(list_res[_id+1][1]) - float(list_res[_id][1])
        resf.write('"' + 'resource stime' + '": ' +
                   '"' + str(resource_stime) + '",\n')

        # minflt_match = re.search(minflt, mm)
        # resource_minflt = minflt_match.group('minflt').decode()
        resource_minflt = float(list_minflt[_id+1]) - float(list_minflt[_id])
        # resource_minflt = float(list_res[_id+1][2]) - float(list_res[_id][2])
        resf.write('"' + 'resource minflt' + '": ' +
                   '"' + str(resource_minflt) + '",\n')

        # majflt_match = re.search(majflt, mm)
        # resource_majflt = majflt_match.group('majflt').decode()
        resource_majflt = float(list_majflt[_id+1]) - float(list_majflt[_id])
        # resource_majflt = float(list_res[_id+1][3]) - float(list_res[_id][3])
        resf.write('"' + 'resource majflt' + '": ' +
                   '"' + str(resource_majflt) + '",\n')

        # inblock_match = re.search(inblock, mm)
        # resource_inblock = inblock_match.group('inblock').decode()
        resource_inblock = float(list_inblock[_id+1]) - float(list_inblock[_id])
        # resource_inblock = float(list_res[_id+1][4]) - float(list_res[_id][4])
        resf.write('"' + 'resource inblock' + '": ' +
                   '"' + str(resource_inblock) + '",\n')

        # oublock_match = re.search(oublock, mm)
        # resource_oublock = oublock_match.group('oublock').decode()
        resource_oublock = float(list_oublock[_id+1]) - float(list_oublock[_id])
        # resource_oublock = float(list_res[_id+1][5]) - float(list_res[_id][5])
        resf.write('"' + 'resource oublock' + '": ' +
                   '"' + str(resource_oublock) + '"\n')

        resf.write('}\n')


def push_resource(list_res, mm, match, _id_):
    list_ = []
    resource_utime = match.group('utime').decode()
    print('resource_utime: ' % resource_utime)
    list_.append(resource_utime)

    stime_match = re.search(stime, mm)
    resource_stime = stime_match.group('stime').decode()
    print('resource_stime: ' % resource_stime)
    list_.append(resource_stime)

    minflt_match = re.search(minflt, mm)
    resource_minflt = minflt_match.group('minflt').decode()

    print('resource_minflt: ' % resource_minflt)
    list_.append(resource_minflt)

    majflt_match = re.search(majflt, mm)
    resource_majflt = majflt_match.group('majflt').decode()
    print('resource_majflt: ' % resource_majflt)
    list_.append(resource_majflt)

    inblock_match = re.search(inblock, mm)
    resource_inblock = inblock_match.group('inblock').decode()
    print('resource_inblock: ' % resource_inblock)
    list_.append(resource_inblock)

    oublock_match = re.search(oublock, mm)
    resource_oublock = oublock_match.group('oublock').decode()
    print('resource_oublock: ' % resource_oublock)
    list_.append(resource_oublock)

    list_res.append(list_)
    return list_res


def main():
    types = [name for name in os.listdir(DIR_LOG)
             if (os.path.isfile(os.path.join(DIR_LOG, name))
                 and name.find('_type_') > 0)]
    types.sort()

    for _type in types:
        print('in file: %s' % _type)
        f = open(os.path.join(DIR_LOG, _type), 'r')
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        # list_res = []
        list_utime = []
        list_stime = []
        list_minflt = []
        list_majflt = []
        list_inblock = []
        list_oublock = []

        for (_id, match) in enumerate(re.finditer(utime, mm)):
            resource_utime = match.group('utime').decode()
            # print(resource_utime)
            list_utime.append(resource_utime)
        mm.seek(0)
        for (_id, match) in enumerate(re.finditer(stime, mm)):
            resource_stime = match.group('stime').decode()
            # print(resource_stime)
            list_stime.append(resource_stime)
        mm.seek(0)
        for (_id, match) in enumerate(re.finditer(minflt, mm)):
            resource_minflt = match.group('minflt').decode()
            # print(resource_minflt)
            list_minflt.append(resource_minflt)
        mm.seek(0)
        for (_id, match) in enumerate(re.finditer(majflt, mm)):
            resource_majflt = match.group('majflt').decode()
            # print(resource_majflt)
            list_majflt.append(resource_majflt)
        mm.seek(0)
        for (_id, match) in enumerate(re.finditer(inblock, mm)):
            resource_inblock = match.group('inblock').decode()
            # print(resource_inblock)
            list_inblock.append(resource_inblock)
        mm.seek(0)
        for (_id, match) in enumerate(re.finditer(oublock, mm)):
            resource_oublock = match.group('oublock').decode()
            # print(resource_oublock)
            list_oublock.append(resource_oublock)
        mm.seek(0)
            # s = match.start()
            # mm.seek(s)
            # list_res = push_resource(list_res, mm, match, _id)
        # record process for certain query type
        # print(list_res)
        # mm.seek(0)

        for (_id, match) in enumerate(re.finditer(process, mm)):
            print('-----parsing %s now-----\n' % match.group(0))
            s = match.start()
            mm.seek(s)
            # log_single(list_res, mm, match, _id)
            log_single(list_utime, list_stime, list_minflt,
                       list_majflt, list_inblock, list_oublock,
                       mm, match, _id)


if __name__ == "__main__":
    main()
