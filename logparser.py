import mmap
import re

LOG_FILE = './log/postgresql-2017-07-24_230930.log'

plan = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2}(\.(\d)+)?)
    .*\]:\[
    (?P<app_name>(\w)+)
    \]:\[
    (?P<process_id>(\d)+)
    \]:\[
    (?P<user_name>sylver)
    \]:\[
    (?P<session_id>.*)
    \]:\[
    (?P<session_line>(\d)+)
    .*duration:\s*
    (?P<duration>(\d)+(\.(\d)+)?)
    .*plan
    ''',
    re.VERBOSE)

end = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2}(\.(\d)+)?)
    ''',
    re.VERBOSE)

result = re.compile(
    rb'''
    (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2}(\.(\d)+)?)
    .*\]:\[
    (?P<app_name>(\w)+)
    \]:\[
    (?P<process_id>(\d)+)
    \]:\[
    (?P<user_name>sylver)
    \]:\[
    (?P<session_id>.*)
    \]:\[
    (?P<session_line>(\d)+)
    .*duration:\s*
    (?P<runtime>(\d)+(\.(\d)+)?)
    .*statement
    .*select
    # .*[^;]$
    ''',
    re.VERBOSE)


def log_plan(mm, match, _id):
    timestamp = match.group('timestamp').decode()
    app_name = match.group('app_name').decode()
    process_id = match.group('process_id').decode()
    username = match.group('user_name').decode()
    session_id = match.group('session_id').decode()
    session_line = match.group('session_line').decode()
    duration = match.group('duration').decode()

    res_name = str(_id) + '_' + process_id + '_' + session_line
    with open('./explain/'+res_name, 'wt') as resf:
        resf.write('{\n')
        resf.write('"' + '_id' + '": ' + '"' + str(_id) + '",\n')
        resf.write('"' + 'timestamp' + '": ' + '"' + timestamp + '",\n')
        resf.write('"' + 'app_name' + '": ' + '"' + app_name + '",\n')
        resf.write('"' + 'process_id' + '": ' + '"' + process_id + '",\n')
        resf.write('"' + 'username' + '": ' + '"' + username + '",\n')
        resf.write('"' + 'session_id' + '": ' + '"' + session_id + '",\n')
        resf.write('"' + 'session_line' + '": ' + '"' + session_line + '",\n')
        resf.write('"' + 'duration' + '": ' + '"' + duration + '",\n')

        mm.readline()
        mm.readline()

        while True:
            line = mm.readline()
            match_end = re.search(end, line)
            if (match_end):
                break
            else:
                resf.write(line.decode())
                continue


def log_res(mm, match, _id):
    timestamp = match.group('timestamp').decode()
    app_name = match.group('app_name').decode()
    process_id = match.group('process_id').decode()
    username = match.group('user_name').decode()
    session_id = match.group('session_id').decode()
    session_line = match.group('session_line').decode()
    runtime = match.group('runtime').decode()

    res_name = str(_id) + '_' + process_id + '_' + session_line
    with open('./explain/'+res_name, 'wt') as resf:
        resf.write('{\n')
        resf.write('"' + '_id' + '": ' + '"' + str(_id) + '",\n')
        resf.write('"' + 'timestamp' + '": ' + '"' + timestamp + '",\n')
        resf.write('"' + 'app_name' + '": ' + '"' + app_name + '",\n')
        resf.write('"' + 'process_id' + '": ' + '"' + process_id + '",\n')
        resf.write('"' + 'username' + '": ' + '"' + username + '",\n')
        resf.write('"' + 'session_id' + '": ' + '"' + session_id + '",\n')
        resf.write('"' + 'session_line' + '": ' + '"' + session_line + '",\n')
        resf.write('"' + 'runtime' + '": ' + '"' + runtime + '"\n')
        resf.write('}\n')


def main():
    f = open(LOG_FILE, 'r')
    mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    for (_id, match) in enumerate(re.finditer(result, mm)):
        print('-----parsing res  %s now-----\n' % match.group(0))
        s = match.start()
        mm.seek(s)
        log_res(mm, match, _id)

    mm.seek(0)

    for (_id, match) in enumerate(re.finditer(plan, mm)):
        print('-----parsing %s now-----\n' % match.group(0))
        s = match.start()
        mm.seek(s)
        log_plan(mm, match, _id)

    # print('before %d \n' % mm.tell())
    # mm.seek(0)
    # print('after %d \n' % mm.tell())
    # match = re.search(result, mm)
    # print(match)


if __name__ == "__main__":
    main()
