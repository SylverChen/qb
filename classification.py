import os
import subprocess
import numpy as np

DIR = '/home/sylver/Projects/env/queryperformance20170717/'
DIR_LOG = DIR + 'explain/'
DIR_RES = DIR + 'res/'
INPUT_FILE = DIR + 'run_records/running'


def mv_log(group_id, count):
    dir_log = os.path.join(DIR_LOG, str(group_id))
    # (log_name, user id)
    logs = [(name, int(name.split('_')[2]))
            for name in os.listdir(dir_log)]

    for user_id in range(count):
        subprocess.run(['mkdir', dir_log + '/' + str(user_id)])

    for log in logs:
        subprocess.run(['mv', os.path.join(dir_log, log[0]),
                        os.path.join(dir_log, str(log[1]))])


def mv_res(group_id):

    dir_res = os.path.join(DIR_RES, str(group_id))
    print('-----now we are moving %s' % dir_res)
    reses = [name for name in os.listdir(dir_res)
             if (os.path.isfile(os.path.join(dir_res, name)))]
    reses.sort()
    print(reses)

    set_reses = set()
    for res in reses:
        parts = res.split('_')
        set_reses.add(parts[3])

    for item in set_reses:
        subprocess.run(['mkdir', dir_res + '/' + item])

    dir_name = [name for name in os.listdir(dir_res)
                if (os.path.isdir(os.path.join(dir_res, name)))]

    for _dir in dir_name:
        reses = [name for name in os.listdir(dir_res)
                 if (os.path.isfile(os.path.join(dir_res, name)))]
        for res in reses:
            parts = res.split('_')
            if parts[3] == _dir:
                subprocess.run(['mv', os.path.join(dir_res, res),
                                os.path.join(dir_res, _dir)])
            else:
                continue


def mv_group_res():
    reses = [name for name in os.listdir(DIR_RES)
             if (os.path.isfile(os.path.join(DIR_RES, name)))]
    reses.sort()

    for res in reses:
        group_str = res.split('_')[1]
        subprocess.run(['mv', os.path.join(DIR_RES, res),
                        os.path.join(DIR_RES, group_str)])


def mv_group_log(user_array, indexes):
    # (log_name, id from pg, process id, user id)
    logs = [(name, int(name.split('_')[0]), int(name.split('_')[1]),
             int(name.split('_')[2]))
            for name in os.listdir(DIR_LOG)
            if (os.path.isfile(os.path.join(DIR_LOG, name)))]

    for group_id in indexes:
        print('group log moving group_id %d now\n' % group_id)
        count = user_array[group_id]
        print('% users in this group\n' % count)
        for user_id in range(count):
            print('processing user %d now\n' % user_id)
            user_logs = [log for log in logs if log[-1] == user_id]
            # if user_logs == []:
            #     continue
            min_id = min(user_logs, key=lambda log: log[1])[1]
            # print(min_id)
            log = [log for log in user_logs if log[1] == min_id][0]
            # print(log)
            process_id = log[2]

            current_logs = [log for log in logs if log[2] == process_id]
            for log in current_logs:
                subprocess.run(['mv', os.path.join(DIR_LOG, log[0]),
                                os.path.join(DIR_LOG, str(group_id))])

                logs.remove(log)

        mv_log(group_id, count)


def main():
    samples = np.loadtxt('samples_24mpl', dtype='int')
    indexes = []
    with open(INPUT_FILE, 'rt') as f:
        for line in f:
            indexes.append(int(line.strip()))
    indexes = indexes[9:12]
    print('-----now we are running-----\n')
    print(indexes)

    # indexes = range(14, 146)
    # indexes = range(14, samples.shape[0])
    user_array = samples.sum(axis=1)
    # check group number and mkdir
    for group_id in indexes:
        subprocess.run(['mkdir', DIR_RES + str(group_id)])
        subprocess.run(['mkdir', DIR_LOG + str(group_id)])

    mv_group_res()
    mv_group_log(user_array, indexes)

    for group_id in indexes:
        mv_res(group_id)


if __name__ == "__main__":
    main()
