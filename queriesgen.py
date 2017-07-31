import itertools
import os
import subprocess

DIR_NAME = '/home/sylver/Projects/env/queryperformance20170717/v2.4.0/'
DIR_QUERY = DIR_NAME + 'query/tpcds20gb/'
DIR_TEMPLATE = DIR_NAME + 'query_templates/'
DIR_TOOL = DIR_NAME + 'tools/'
MPL = 24


def main():
    types = [name for name in os.listdir(DIR_QUERY)
             if (os.path.isdir(os.path.join(DIR_QUERY, name))
                 and name.startswith('type_'))]
    for (_id, pair) in enumerate(itertools.product(types, range(MPL))):
        _dir = DIR_QUERY + pair[0] + '/' + str(pair[1])
        subprocess.run(['mkdir', _dir])
        _type = pair[0].split('_')[-1]
        subprocess.run([DIR_TOOL+'dsqgen', '-template', 'query'+_type+'.tpl',
                        '-directory', DIR_TEMPLATE, '-dialect', 'postgres',
                        '-scale', '20', '-output_dir', _dir, '-stream', '40'])


if __name__ == '__main__':
    main()
