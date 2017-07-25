import mmap
import re
import os

DIR_LOG = './log/'
process = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2}(\,(\d)+)?)
    .*\[
    (?P<_id>(\d)+)
    \]:\[
    (?P<script>.*sql)
    \]:\[
    (?P<process_id>(\w)+)
    \]:\[
    (?P<type>type_(\d)+)
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
    (?P<utime>(\d)+(\.(\d)+)?)
    ''',
    re.VERBOSE)
stime = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2})
    .*ru_stime
    .*
    (?P<stime>(\d)+(\.(\d)+)?)
    ''',
    re.VERBOSE)
minflt = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2})
    .*ru_minflt
    .*
    (?P<minflt>(\d)+(\.(\d)+)?)
    ''',
    re.VERBOSE)
majflt = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2})
    .*ru_majflt
    .*
    (?P<majflt>(\d)+(\.(\d)+)?)
    ''',
    re.VERBOSE)
inblock = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2})
    .*ru_inblock
    .*
    (?P<inblock>(\d)+(\.(\d)+)?)
    ''',
    re.VERBOSE)
oublock = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2})
    .*ru_oublock
    .*
    (?P<oublock>(\d)+(\.(\d)+)?)
    ''',
    re.VERBOSE)


def log_single(list_res, mm, match, _id):
    _number = match.group('_id').decode()
    script = match.group('script').decode()
    process_id = match.group('process_id').decode()
    _type = match.group('type').decode()

    res_name = str(_id) + '_' + process_id + '_' + _type + '_' + script[:-4]
    with open('./res/'+res_name, 'wt') as resf:
        resf.write('{\n')
        resf.write('"' + '_id' + '": ' + '"' + str(_number) + '",\n')
        # resf.write('"' + 'Timestamp' + '": ' + '"' + timestamp + '",\n')
        resf.write('"' + 'Process Id' + '": ' + '"' + process_id + '",\n')
        resf.write('"' + 'Script Name' + '": ' + '"' + script + '",\n')
        resf.write('"' + 'Query Type' + '": ' + '"' + _type + '",\n')

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
        resource_utime = float(list_res[_id+1][0]) - float(list_res[_id][0])
        resf.write('"' + 'resource utime' + '": ' +
                   '"' + str(resource_utime) + '",\n')

        # stime_match = re.search(stime, mm)
        # resource_stime = stime_match.group('stime').decode()
        resource_stime = float(list_res[_id+1][1]) - float(list_res[_id][1])
        resf.write('"' + 'resource stime' + '": ' +
                   '"' + str(resource_stime) + '",\n')

        # minflt_match = re.search(minflt, mm)
        # resource_minflt = minflt_match.group('minflt').decode()
        resource_minflt = float(list_res[_id+1][2]) - float(list_res[_id][2])
        resf.write('"' + 'resource minflt' + '": ' +
                   '"' + str(resource_minflt) + '",\n')

        # majflt_match = re.search(majflt, mm)
        # resource_majflt = majflt_match.group('majflt').decode()
        resource_majflt = float(list_res[_id+1][3]) - float(list_res[_id][3])
        resf.write('"' + 'resource majflt' + '": ' +
                   '"' + str(resource_majflt) + '",\n')

        # inblock_match = re.search(inblock, mm)
        # resource_inblock = inblock_match.group('inblock').decode()
        resource_inblock = float(list_res[_id+1][4]) - float(list_res[_id][4])
        resf.write('"' + 'resource inblock' + '": ' +
                   '"' + str(resource_inblock) + '",\n')

        # oublock_match = re.search(oublock, mm)
        # resource_oublock = oublock_match.group('oublock').decode()
        resource_oublock = float(list_res[_id+1][5]) - float(list_res[_id][5])
        resf.write('"' + 'resource oublock' + '": ' +
                   '"' + str(resource_oublock) + '"\n')

        resf.write('}\n')


def push_resource(list_res, mm, match, _id_):
    list_ = []
    resource_utime = match.group('utime').decode()
    list_.append(resource_utime)

    stime_match = re.search(stime, mm)
    resource_stime = stime_match.group('stime').decode()
    list_.append(resource_stime)

    minflt_match = re.search(minflt, mm)
    resource_minflt = minflt_match.group('minflt').decode()
    list_.append(resource_minflt)

    majflt_match = re.search(majflt, mm)
    resource_majflt = majflt_match.group('majflt').decode()
    list_.append(resource_majflt)

    inblock_match = re.search(inblock, mm)
    resource_inblock = inblock_match.group('inblock').decode()
    list_.append(resource_inblock)

    oublock_match = re.search(oublock, mm)
    resource_oublock = oublock_match.group('oublock').decode()
    list_.append(resource_oublock)

    list_res.append(list_)
    return list_res


def main():
    types = [name for name in os.listdir(DIR_LOG)
             if (os.path.isfile(os.path.join(DIR_LOG, name))
                 and name.startswith('type_'))]
    types.sort()

    for _type in types:
        f = open(os.path.join(DIR_LOG, _type), 'r')
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        list_res = []

        for (_id, match) in enumerate(re.finditer(utime, mm)):
            print('-----parsing resource %s now-----\n' % match.group(0))
            s = match.start()
            mm.seek(s)
            list_res = push_resource(list_res, mm, match, _id)
        # record process for certain query type
        print(list_res)
        mm.seek(0)

        for (_id, match) in enumerate(re.finditer(process, mm)):
            print('-----parsing %s now-----\n' % match.group(0))
            s = match.start()
            mm.seek(s)
            log_single(list_res, mm, match, _id)


if __name__ == "__main__":
    main()
