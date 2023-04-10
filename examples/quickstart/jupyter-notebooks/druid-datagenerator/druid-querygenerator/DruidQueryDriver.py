# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# DruidQueryDriver - generates SQL queries as a query workload for Apache Druid.
#

import argparse
import dateutil.parser
from datetime import datetime, timedelta
import json
import numpy as np
import random
import requests
import string
import sys
import threading
import time

############################################################################
#
# DruidQueryDriver simulates Druid query workloads by producing SQL queries
# to Druid endpoints.
# Use a JSON config file to describe the characteristics of the query
# workload
#
# Run the program as follows:
# python DruidQueryDriver.py -f <config file name> <options>
# Options include:
# -n <total number of queries to generate>
# -t <duration for generating queries>
#
# See the associated documentation for the format of the config file.
#
############################################################################


#
# Handle distributions
#

class DistConstant:
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return 'DistConstant(value='+str(self.value)+')'
    def get_sample(self):
        return self.value

class DistUniform:
    def __init__(self, min_value, max_value):
        self.min_value = min_value
        self.max_value = max_value
    def __str__(self):
        return 'DistUniform(min_value='+str(self.min_value)+', max_value='+str(self.max_value)+')'
    def get_sample(self):
        return np.random.uniform(self.min_value, self.max_value+1)

class DistExponential:
    def __init__(self, mean):
        self.mean = mean
    def __str__(self):
        return 'DistExponential(mean='+str(self.mean)+')'
    def get_sample(self):
        return np.random.exponential(scale = self.mean)

class DistNormal:
    def __init__(self, mean, stddev):
        self.mean = mean
        self.stddev = stddev
    def __str__(self):
        return 'DistNormal(mean='+str(self.mean )+', stddev='+str(self.stddev)+')'
    def get_sample(self):
        return np.random.normal(self.mean, self.stddev)

class DistNow:
    def __str__(self):
        return 'DistNow()'
    def get_sample(self):
        return datetime.now().isoformat()[:-3]

class DistMinus:
    def __init__(self, base, delta):
        self.base = base
        self.delta = delta
    def get_sample(self):
        base_sample = dateutil.parser.isoparse(self.base.get_sample())
        delta_sample = self.delta.get_sample()
        t = base_sample - timedelta(seconds=delta_sample)
        return t.isoformat()[:-3]

class DistConstantTime:
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return 'DistConstantTime(value='+str(self.value)+')'
    def get_sample(self):
        print('value = '+str(self.value))
        return dateutil.parser.isoparse(self.value).isoformat()[:-3]

class DistUniformTime:
    def __init__(self, min_value, max_value):
        self.min_value = min_value
        self.max_value = max_value
    def __str__(self):
        return 'DistUniformTime(min_value='+str(self.min_value)+', max_value='+str(self.max_value)+')'
    def get_sample(self):
        return datetime.fromtimestamp(np.random.uniform(dateutil.parser.isoparse(self.min_value.get_sample()).timestamp(), dateutil.parser.isoparse(self.max_value.get_sample()).timestamp())).isoformat()[:-3]

class DistExponentialTime:
    def __init__(self, mean):
        self.mean = mean
    def __str__(self):
        return 'DistExponentialTime(mean='+str(self.mean)+')'
    def get_sample(self):
        return datetime.fromtimestamp(np.random.exponential(scale = dateutil.parser.isoparse(self.mean.get_sample()).timestamp())).isoformat()[:-3]

class DistNormalTime:
    def __init__(self, mean, stddev):
        self.mean = mean
        self.stddev = stddev
    def __str__(self):
        return 'DistNormalTime(mean='+str(self.mean )+', stddev='+str(self.stddev)+')'
    def get_sample(self):
        return datetime.fromtimestamp(np.random.normal(dateutil.parser.isoparse(self.mean.get_sample()).timestamp(), self.stddev)).isoformat()[:-3]

def parse_distribution(desc):
    dist_type = desc['type'].lower()
    dist_gen = None
    if dist_type == 'constant':
        value = desc['value']
        dist_gen = DistConstant(value)
    elif dist_type == 'uniform':
        min_value = desc['min']
        max_value = desc['max']
        dist_gen = DistUniform(min_value, max_value)
    elif dist_type == 'exponential':
        mean = desc['mean']
        dist_gen = DistExponential(mean)
    elif dist_type == 'normal':
        mean = desc['mean']
        stddev = desc['stddev']
        dist_gen = DistNormal(mean, stddev)
    else:
        print('Error: Unknown distribution "'+dist_type+'"')
        exit()
    return dist_gen

def parse_timestamp_distribution(desc):
    dist_type = desc['type'].lower()
    dist_gen = None
    if dist_type == 'constant':
        value = desc['value']
        dist_gen = DistConstantTime(value)
    elif dist_type == 'uniform':
        min_value = parse_timestamp_distribution(desc['min'])
        max_value = parse_timestamp_distribution(desc['max'])
        dist_gen = DistUniformTime(min_value, max_value)
    elif dist_type == 'exponential':
        mean = parse_timestamp_distribution(desc['mean'])
        dist_gen = DistExponentialTime(mean)
    elif dist_type == 'normal':
        mean = parse_timestamp_distribution(desc['mean'])
        stddev = desc['stddev']
        dist_gen = DistNormalTime(mean, stddev)
    elif dist_type == 'now':
        dist_gen = DistNow()
    elif dist_type == 'minus':
        base = parse_timestamp_distribution(desc['base'])
        delta = parse_distribution(desc['delta'])
        dist_gen = DistMinus(base, delta)
    else:
        print('Error: Unknown distribution "'+dist_type+'"')
        exit()
    return dist_gen





class VarBase:
    def __init__(self, desc):
        self.name = desc['name']
        self.cardinality = None
        cardinality = desc['cardinality']
        if cardinality == 0:
            self.cardinality_distribution = None
        else:
            if 'cardinality_distribution' not in desc.keys():
                print('Variable '+self.name+' specifies a cardinality without a cardinality distribution')
                exit()
            card_list = []
            for i in range(cardinality):
                Value = None
                while True:
                    value = self.get_stochastic_value()
                    if value not in card_list:
                        break
                card_list.append(value)
            self.cardinality = card_list
            self.cardinality_distribution = parse_distribution(desc['cardinality_distribution'])

    def get_stochastic_value(self):
        pass

class VarEnum(VarBase):
    def __init__(self, desc):
        self.name = desc['name']
        self.cardinality = desc['values']
        if 'cardinality_distribution' not in desc.keys():
            print('Variable '+self.name+' specifies a cardinality without a cardinality distribution')
            exit()
        self.cardinality_distribution = parse_distribution(desc['cardinality_distribution'])

    def __str__(self):
        return 'VarEnum(name='+self.name+', cardinality='+str(self.cardinality)+', cardinality_distribution='+str(self.cardinality_distribution)+')'

    def get_stochastic_value(self):
        index = int(self.cardinality_distribution.get_sample())
        if index < 0:
            index = 0
        if index >= len(self.cardinality):
            index = len(self.cardinality)-1
        return self.cardinality[index]

class VarString(VarBase):
    def __init__(self, desc):
        self.length_distribution = parse_distribution(desc['length_distribution'])
        if 'chars' in desc:
            self.chars = desc['chars']
        else:
            self.chars = string.printable
        super().__init__(desc)

    def __str__(self):
        return 'VarString(name='+self.name+', cardinality='+str(self.cardinality)+', cardinality_distribution='+str(self.cardinality_distribution)+', chars='+self.chars+')'

    def get_stochastic_value(self):
        length = int(self.length_distribution.get_sample())
        return ''.join(random.choices(list(self.chars), k=length))

class VarInt(VarBase):
    def __init__(self, desc):
        self.value_distribution = parse_distribution(desc['distribution'])
        super().__init__(desc)

    def __str__(self):
        return 'VarInt(name='+self.name+', value_distribution='+str(self.value_distribution)+', cardinality='+str(self.cardinality)+', cardinality_distribution='+str(self.cardinality_distribution)+')'

    def get_stochastic_value(self):
        return str(int(self.value_distribution.get_sample()))

class VarFloat(VarBase):
    def __init__(self, desc):
        self.value_distribution = parse_distribution(desc['distribution'])
        if 'precision' in desc:
            self.precision = desc['precision']
        else:
            self.precision = None
        super().__init__(desc)

    def __str__(self):
        return 'VarFloat(name='+self.name+', value_distribution='+str(self.value_distribution)+', cardinality='+str(self.cardinality)+', cardinality_distribution='+str(self.cardinality_distribution)+')'

    def get_stochastic_value(self):
        if self.cardinality is None:
            value = float(self.value_distribution.get_sample())
            if self.precision is None:
                s = str(value)
            else:
                format = '%.'+str(self.precision)+'f'
                s = str(format%value)
        else:
            index = int(self.cardinality_distribution.get_sample())
            if index < 0:
                index = 0
            if index >= len(self.cardinality):
                index = len(self.cardinality)-1
            s = self.cardinality[index]
        return s

        return float(self.value_distribution.get_sample())

class VarTimestamp(VarBase):
    def __init__(self, desc):
        self.name = desc['name']
        self.value_distribution = parse_timestamp_distribution(desc['distribution'])
        cardinality = desc['cardinality']
        if cardinality == 0:
            self.cardinality = None
            self.cardinality_distribution = None
        else:
            if 'cardinality_distribution' not in desc.keys():
                print('Variable '+self.name+' specifies a cardinality without a cardinality distribution')
                exit()
            self.cardinality = []
            self.cardinality_distribution = parse_distribution(desc['cardinality_distribution'])
            for i in range(cardinality):
                Value = None
                while True:
                    value = self.get_stochastic_value()
                    if value not in self.cardinality:
                        break
                self.cardinality.append(value)

    def __str__(self):
        return 'VarTimestamp(name='+self.name+', value_distribution='+str(self.value_distribution)+', cardinality='+str(self.cardinality)+', cardinality_distribution='+str(self.cardinality_distribution)+')'

    def get_stochastic_value(self):
        return self.value_distribution.get_sample()

class VarIPAddress(VarBase):
    def __init__(self, desc):
        self.value_distribution = parse_distribution(desc['distribution'])
        super().__init__(desc)

    def __str__(self):
        return 'VarIPAddress(name='+self.name+', value_distribution='+str(self.value_distribution)+', cardinality='+str(self.cardinality)+', cardinality_distribution='+str(self.cardinality_distribution)+')'

    def get_stochastic_value(self):
        value = int(self.value_distribution.get_sample())
        return str((value & 0xFF000000) >> 24)+'.'+str((value & 0x00FF0000) >> 16)+'.'+str((value & 0x0000FF00) >> 8)+'.'+str(value & 0x000000FF)


class SimEnd:
    lock = threading.Lock()
    thread_end_event = threading.Event()
    query_count = 0
    def __init__(self, total_queries, runtime):
        self.total_queries = total_queries
        if runtime is None:
            self.t = None
        else:
            if runtime[-1].lower() == 's':
                self.t = int(runtime[:-1])
            elif runtime[-1].lower() == 'm':
                self.t = int(runtime[:-1]) * 60
            elif runtime[-1].lower() == 'h':
                self.t = int(runtime[:-1]) * 60 * 60
            else:
                print('Error: Unknown runtime value"'+runtime+'"')
                exit()

    def inc_query_count(self):
        self.lock.acquire()
        self.query_count += 1
        self.lock.release()
        if (self.total_queries is not None) and (self.query_count >= self.total_queries):
            self.thread_end_event.set()

    def is_done(self):
        return ((self.total_queries is not None) and (self.query_count >= self.total_queries)) or ((self.t is not None) and self.thread_end_event.is_set())

    def wait_for_end(self):
        if self.t is not None:
            time.sleep(self.t)
            self.thread_end_event.set()
        elif self.total_queries is not None:
            self.thread_end_event.wait()
        else:
            while True:
                time.sleep(60)

def get_variable(desc):
    if desc['type'].lower() == 'enum':
        v = VarEnum(desc)
    elif desc['type'].lower() == 'string':
        v = VarString(desc)
    elif desc['type'].lower() == 'int':
        v = VarInt(desc)
    elif desc['type'].lower() == 'float':
        v = VarFloat(desc)
    elif desc['type'].lower() == 'timestamp':
        v = VarTimestamp(desc)
    elif desc['type'].lower() == 'ipaddress':
        v = VarIPAddress(desc)
    else:
        print('Error: Unknown variable type "'+desc['type']+'"')
        exit()
    return v

def get_variables(var_list):
    variables = {}
    for v in var_list:
        variables[v['name']] = get_variable(v)
    return variables

def expand_query(raw_query, variables):
    result = raw_query
    for key in variables:
        var_name = '$'+key
        var_value = variables[key].get_stochastic_value()
        result = result.replace(var_name, var_value)

    return result


def post_query(target, query, target_stdout, target_url):

    if target_stdout:
        print('{"query": "'+query+'"}')
    if target_url:
        req = requests.post(target, json={"query": query})
        if req.status_code != 200:
           sys.stderr.write('ERROR ('+query+'): '+str(req.status_code)+'\n')

def query_thread(query, sim_end, target_stdout, target_url):

    target = query['target']
    delay = parse_distribution(query['delay'])
    raw_query = query['query']
    variables = get_variables(query['variables'])
    while True:
        delta = float(delay.get_sample())
        time.sleep(delta)
        if sim_end.is_done():
            break
        expanded_query = expand_query(raw_query, variables)
        post_query(target, expanded_query, target_stdout, target_url)
        sim_end.inc_query_count()

def simulate_queries(config_file_name, runtime, total_queries, target_stdout, target_url):
    if config_file_name:
        with open(config_file_name, 'r') as f:
            config = json.load(f)
    else:
        config = json.load(sys.stdin)

    sim_end = SimEnd(total_queries, runtime)

    for query in config['queries']:
        t = threading.Thread(target=query_thread, args=(query, sim_end, target_stdout, target_url, ), name=query['query'], daemon=True)
        t.start()

    sim_end.wait_for_end()


def main():

    #
    # Parse the command line
    #

    parser = argparse.ArgumentParser(description='Generates SQL queries as a query workload for Apache Druid.')
    parser.add_argument('-f', dest='config_file', nargs='?', help='the workload config file name')
    parser.add_argument('-o', dest='target', nargs='?', help='Log queries to stdout (values include TARGET, STDOUT, BOTH)')
    parser.add_argument('-t', dest='time', nargs='?', help='the script runtime (may not be used with -n)')
    parser.add_argument('-n', dest='n_queries', nargs='?', help='the number of records to generate (may not be used with -t)')

    args = parser.parse_args()

    config_file_name = args.config_file
    runtime = args.time
    total_queries = None
    if args.n_queries is not None:
        total_queries = int(args.n_queries)

    if args.target is not None:
        if args.target.lower() == 'stdout':
            target_stdout = True
            target_url = False
        elif args.target.lower() == 'target':
            target_stdout = False
            target_url = True
        elif args.target.lower() == 'both':
            target_stdout = True
            target_url = True
        else:
            print('Unrecognized value ("'+args.target+'") with -o flag - should be TARGET, STDOUT or BOTH')
            exit()
    else:
        target_stdout = False
        target_url = True

    if (runtime is not None) and (total_queries is not None):
        print("Use either -t or -n, but not both")
        parser.print_help()
        exit()

    simulate_queries(config_file_name, runtime, total_queries, target_stdout, target_url)

if __name__ == "__main__":
    main()
