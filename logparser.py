import mmap
import re


LOG_FILE = './log/postgresql-2017-07-23_114301.log'


def log_single(mm, match, header_end, _id):
    # all fields are of type bytearray
    timestamp = match.group('timestamp').decode()
    app_name = match.group('app_name').decode()
    process_id = match.group('process_id').decode()
    user_name = match.group('user_name').decode()
    session_id = match.group('session_id').decode()
    session_line = match.group('session_line').decode()
    duration = match.group('duration').decode()

    res_name = str(_id) + '_' + process_id + '_' + session_line
    with open('./res/'+res_name, 'wt') as resf:
        resf.write('{\n')
        resf.write('"' + '_id' + '": ' + '"' + str(_id) + '",\n')
        resf.write('"' + 'Timestamp' + '": ' + '"' + timestamp + '",\n')
        resf.write('"' + 'Application Name' + '": ' + '"' + app_name + '",\n')
        resf.write('"' + 'Process Id' + '": ' + '"' + process_id + '",\n')
        resf.write('"' + 'Username' + '": ' + '"' + user_name + '",\n')
        resf.write('"' + 'Session Id' + '": ' + '"' + session_id + '",\n')
        resf.write('"' + 'Session line' + '": ' + '"' + session_line + '",\n')
        resf.write('"' + 'Duration' + '": ' + '"' + duration + '",\n')

        for i in range(3):
            mm.readline()

        while True:
            # line = mm.readline().decode()
            line = mm.readline()
            if ('' == line or header_end.search(line)):
                break
            # if (header_end.search(line)):
            #     break
            else:
                resf.write(line.decode())


def log_res(mm, match, header_res, _id):
    # all fields are of type bytearray
    timestamp = match.group('timestamp').decode()
    app_name = match.group('app_name').decode()
    process_id = match.group('process_id').decode()
    user_name = match.group('user_name').decode()
    session_id = match.group('session_id').decode()
    session_line = match.group('session_line').decode()
    duration = match.group('duration').decode()

    res_name = str(_id) + '_' + process_id + '_' + session_line
    with open('./res/'+res_name, 'wt') as resf:
        resf.write('{\n')
        resf.write('"' + '_id' + '": ' + '"' + str(_id) + '",\n')
        resf.write('"' + 'Timestamp' + '": ' + '"' + timestamp + '",\n')
        resf.write('"' + 'Application Name' + '": ' + '"' + app_name + '",\n')
        resf.write('"' + 'Process Id' + '": ' + '"' + process_id + '",\n')
        resf.write('"' + 'Username' + '": ' + '"' + user_name + '",\n')
        resf.write('"' + 'Session Id' + '": ' + '"' + session_id + '",\n')
        resf.write('"' + 'Session line' + '": ' + '"' + session_line + '",\n')
        resf.write('"' + 'Duration' + '": ' + '"' + duration + '"\n')
        resf.write('}\n')


def main():
    f = open(LOG_FILE, 'r')
    mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    header = re.compile(
        rb'''
        (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2}(\.(\d)+)?)
        \s*CST\]:\[
        (?P<app_name>(\w)+)
        \]:\[
        (?P<process_id>(\d)+)
        \]:\[
        # (?P<user_name>(\w)+)
        (?P<user_name>sylver)
        \]:\[
        (?P<session_id>.*)
        \]:\[
        (?P<session_line>(\d)+)
        .*duration:\s*
        (?P<duration>(\d)+(\.(\d)+)?)
        .*plan:
        ''',
        re.VERBOSE)

    header_end = re.compile(
        rb'''
        (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2}(\.(\d)+)?)
        \s*CST\]:\[
        (?P<app_name>(\w)+)
        \]:\[
        (?P<process_id>(\d)+)
        \]:\[
        (?P<user_name>(\w)+)
        \]:\[
        (?P<session_id>.*)
        \]:\[
        (?P<session_line>(\d)+)
        .*duration:\s*
        (?P<duration>(\d)+(\.(\d)+)?)
        ''',
        re.VERBOSE)

    header_res = re.compile(
        rb'''
        (?P<timestamp>(\d){4}\-(\d){2}\-(\d){2}\s(\d){2}:(\d){2}:(\d){2})
        \s*CST\]:\[
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
        .*statement:
        ''',
        re.VERBOSE)

    # record explain
    for (_id, match) in enumerate(re.finditer(header, mm)):
        print('-----parsing %s now-----\n' % match.group(0))
        s = match.start()
        mm.seek(s)
        log_single(mm, match, header_end, _id)

    # record run time
    for (_id, match) in enumerate(re.finditer(header_res, mm)):
        print('-----parsing %s now-----\n' % match.group(0))
        s = match.start()
        mm.seek(s)
        log_res(mm, match, header_end, _id)


if __name__ == "__main__":
    main()
