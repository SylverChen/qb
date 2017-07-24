import os
from pprint import pprint
from collections import defaultdict
import json
import logging
import flatdict

# RAW_FILE = 'log/try_json'
RAW_FILE = 'res/8144_2'
DIR_NAME = '/home/sylver/Projects/env/queryperformance20170717/res/'
OUT_NAME = '/home/sylver/Projects/env/queryperformance20170717/'
# Initialze logger
LOG_FILENAME = 'extract_log'

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

    # pprint(d)


def main():

    logs = [name for name in os.listdir(DIR_NAME)
            if (os.path.isfile(os.path.join(DIR_NAME, name)))]
    logs.sort()
    print(logs)
    d_features = dict()
    # with open(RAW_FILE, 'r') as f:
    for log in logs:
        print('extracting log file %s now!' % log)
        logger.info('extracting log file %s now!' % log)
        with open(os.path.join(DIR_NAME, log), 'r') as f:
            data = json.load(f)

        # decide it's input feature or output feature
        if (len(data) == 1):
            d_features[log]['Run Time'] = data['Run Time']
            continue

        flat_data = flatdict.FlatDict(data)
        d_origin = dict()
        d = defaultdict(list)
        extract_single(d_origin, d, flat_data)

        d_feature = dict()
        for (key, value) in d.items():
            feature = dict()
            feature['rows'] = 0
            feature['cost'] = 0
            for item in value:
                feature['rows'] += item['Plan Rows']
                feature['cost'] += item['Total Cost'] - item['Startup Cost']

            d_feature[key] = feature

        d_feature['Timestamp'] = flat_data['Timestamp']
        d_feature['Application Name'] = flat_data['Application Name']
        d_feature['Process Id'] = flat_data['Process Id']
        d_feature['Username'] = flat_data['Username']
        d_feature['Session Id'] = flat_data['Session Id']
        d_feature['Session line'] = flat_data['Session line']
        d_feature['Duration'] = flat_data['Duration']

        key = d_feature['Process Id'] + '_' + d_feature['Session Id'] + '_' + d_feature['Session line']
        d_features[key] = d_feature

    # return d_features
    # pprint(d_features)
    # need to be test
    with open(os.path.join(OUT_NAME, 'features'), 'w') as f:
        json_data = json.dumps(d_features)
        f.write(json_data)
        # pprint(json_data)
        # json.dumps(d_features, f)


if __name__ == "__main__":
    main()
