import os
from pprint import pprint
from collections import defaultdict
import json
import logging
import flatdict

# RAW_FILE = 'log/try_json'
RAW_FILE = 'res/8144_2'
DIR_NAME = '/home/sylver/Projects/env/queryperformance20170717/'
DIR_LOG = DIR_NAME + 'explain/'
DIR_RES = DIR_NAME + 'res/'
OUT_NAME = '/home/sylver/Projects/env/queryperformance20170717/'
# Initialze logger
LOG_FILENAME = 'extract_log'
CORRES = [(29573, 17), (29576, 13), (29596, 72),
          (29598, 18), (29602, 9), (29603, 87)]

logger = logging.getLogger('__name__')
logger.setLevel(logging.DEBUG)

handler = logging.FileHandler(LOG_FILENAME)
handler.setLevel(logging.DEBUG)


def extract_single(d_origin, d, flat_data):
    for (key, value) in flat_data.items():
        if key.endswith('Node Type'):
            newkey = key[:key.find('Node Type')]
            d_origin[newkey] = value

    for (out_key, out_value) in d_origin.items():
        item = dict()
        for (in_key, in_value) in flat_data.items():
            pos = in_key.rfind(':') + 1
            if (out_key == in_key[:pos]):
                item[in_key[pos:]] = in_value
        d[out_value].append(item)


def main():
    d_features = dict()
    d_features['input'] = []
    d_features['output'] = []

    for (explain, types) in CORRES:
        dir_log = os.path.join(DIR_LOG, str(explain))
        reses = [name for name in os.listdir(DIR_RES)
                 if name.find('_'+str(types)) > 0]
        assert len(reses) == 1
        dir_res = os.path.join(DIR_RES, reses[0])

        logger.info('-----now the dir_log is %s-----\n' % dir_log)
        logger.info('-----now the dir_res is %s-----\n' % dir_res)
        print('-----now the dir_log is %s-----\n' % dir_log)
        print('-----now the dir_res is %s-----\n' % dir_res)

        logs = [name for name in os.listdir(dir_log)]
        # logs.sort(key=lambda x: (int(x.split('_')[0]), int(x.split('_')[-1])))
        logs.sort(key=lambda x: int(x.split('_')[-1]))

        reses = [name for name in os.listdir(dir_res)]
        reses.sort(key=lambda x: int(x.split('_')[0]))
        assert len(logs) == 2 * len(reses)
        plans = logs[::2]
        facts = logs[1::2]

        paires = zip(plans, facts, reses)
        for (plan, fact, res) in paires:

            logger.info('----------now the plan is %s-----\n' % plan)
            logger.info('----------now the fact is %s-----\n' % fact)
            logger.info('----------now the res is %s-----\n' % res)
            print('----------now the plan is %s-----\n' % plan)
            print('----------now the fact is %s-----\n' % fact)
            print('----------now the res is %s-----\n' % res)
            input_feature = dict()
            output_feature = dict()

            with open(os.path.join(dir_log, plan), 'r') as f_plan:
                data_plan = json.load(f_plan)
                flat_data = flatdict.FlatDict(data_plan)
                d_origin = dict()
                d = defaultdict(list)
                extract_single(d_origin, d, flat_data)

                for (key, value) in d.items():
                    feature = dict()
                    feature['rows'] = 0
                    feature['cost'] = 0
                    for item in value:
                        feature['rows'] += item['Plan Rows']
                        feature['cost'] += item['Total Cost'] - item['Startup Cost']

                    input_feature[key] = feature

                input_feature['_id'] = flat_data['_id']
                input_feature['timestamp'] = flat_data['timestamp']
                input_feature['app_name'] = flat_data['app_name']
                input_feature['process_id'] = flat_data['process_id']
                input_feature['username'] = flat_data['username']
                input_feature['session_id'] = flat_data['session_id']
                input_feature['session_line'] = flat_data['session_line']
                input_feature['duration'] = flat_data['duration']

            with open(os.path.join(dir_log, fact), 'r') as f_fact:
                data_fact = json.load(f_fact)
                output_feature['_id'] = data_fact['_id']
                output_feature['runtime'] = data_fact['runtime']

            with open(os.path.join(dir_res, res), 'r') as f_res:
                data_res = json.load(f_res)
                input_feature['script'] = data_res['Script Name']
                input_feature['query_type'] = data_res['Query Type']
                input_feature['cpu_user'] = data_res['cpu user']
                input_feature['cpu_system'] = data_res['cpu system']
                input_feature['cpu_idle'] = data_res['cpu idle']
                input_feature['cpu_iowait'] = data_res['cpu iowait']
                input_feature['cpu_irq'] = data_res['cpu irq']
                input_feature['cpu_softirq'] = data_res['cpu softirq']
                input_feature['mem_used'] = data_res['mem used']
                input_feature['mem_free'] = data_res['mem free']
                input_feature['mem_buffers'] = data_res['mem buffers']
                input_feature['mem_cached'] = data_res['mem cached']

                output_feature['utime'] = data_res['resource utime']
                output_feature['stime'] = data_res['resource stime']
                output_feature['minflt'] = data_res['resource minflt']
                output_feature['majflt'] = data_res['resource majflt']
                output_feature['inblock'] = data_res['resource inblock']
                output_feature['oublock'] = data_res['resource oublock']

            d_features['input'].append(input_feature)
            d_features['output'].append(output_feature)

    pprint(d_features)
    with open(os.path.join(OUT_NAME, 'features'), 'w') as f:
        json_data = json.dumps(d_features)
        f.write(json_data)
        # pprint(json_data)
        # json.dumps(d_features, f)


if __name__ == "__main__":
    main()
