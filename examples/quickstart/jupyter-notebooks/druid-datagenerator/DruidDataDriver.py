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
# DruidDataDriver - generates JSON records as a workload for Apache Druid.
#

import argparse
from confluent_kafka import Producer
import dateutil.parser
from datetime import datetime, timedelta
import json
from kafka import KafkaProducer
import numpy as np
import random
import re
from sortedcontainers import SortedList
import string
import sys
import threading
import time

############################################################################
#
# DruidDataDriver simulates Druid workloads by producing JSON records.
# Use a JSON config file to describe the characteristics of the workload
# you want to simulate.
#
# Run the program as follows:
# python DruidDataDriver.py <config file name> <options>
# Options include:
# -n <total number of records to generate>
# -t <duration for generating records>
#
# See the associated documentation for the format of the config file.
#
############################################################################


class FutureEvent:
    def __init__(self, t):
        self.t = t
        self.name = threading.current_thread().name
        self.event = threading.Event()
    def get_time(self):
        return self.t
    def get_name(self):
        return self.name
    def __lt__(self, other):
        return self.t < other.t
    def __eq__(self, other):
        return self.t == other.t
    def __str__(self):
        return 'FutureEvent('+self.name+', '+str(self.t)+')'
    def pause(self):
        #print(self.name+" pausing")
        self.event.clear()
        self.event.wait()
    def resume(self):
        #print(self.name+" resuming")
        self.event.set()

class Clock:
    future_events = SortedList()
    active_threads = 0
    lock = threading.Lock()
    sleep_lock = threading.Lock()

    def __init__(self, time_type, start_time = datetime.now()):
        self.sim_time = start_time
        self.time_type = time_type

    def __str__(self):
        s = 'Clock(time='+str(self.sim_time)
        for e in self.future_events:
            s += ', '+str(e)
        s += ')'
        return s

    def activate_thread(self):
        if self.time_type == 'SIM':
            self.lock.acquire()
            self.active_threads += 1
            self.lock.release()

    def deactivate_thread(self):
        if self.time_type == 'SIM':
            self.lock.acquire()
            self.active_threads -= 1
            self.lock.release()

    def end_thread(self):
        if self.time_type == 'SIM':
            self.lock.acquire()
            self.active_threads -= 1
            if len(self.future_events) > 0:
                self.remove_event().resume()
            self.lock.release()

    def release_all(self):
        if self.time_type == 'SIM':
            self.lock.acquire()
            #print('release_all - active_threads = '+str(self.active_threads))
            for e in self.future_events:
                e.resume()
            self.lock.release()

    def add_event(self, future_t):
        this_event = FutureEvent(future_t)
        self.future_events.add(this_event)
        #print('add_event (after) '+threading.current_thread().name+' - '+str(self))
        return this_event

    def remove_event(self):
        #print('remove_event (before) '+threading.current_thread().name+' - '+str(self))
        next_event = self.future_events[0]
        self.future_events.remove(next_event)
        return next_event

    def pause(self, event):
        self.active_threads -= 1
        self.lock.release()
        event.pause()
        self.lock.acquire()
        self.active_threads += 1

    def resume(self, event):
        event.resume()

    def now(self):
        if self.time_type == 'SIM':
            t = self.sim_time
        else:
            t = datetime.now()
        return t

    def sleep(self, delta):
        if self.time_type == 'SIM': # Simulated time
            self.lock.acquire()
            #print(threading.current_thread().name+" begin sleep "+str(self.sim_time)+" + "+str(delta))
            this_event = self.add_event(self.sim_time + timedelta(seconds=delta))
            #print(threading.current_thread().name+" active threads "+str(self.active_threads))
            if self.active_threads == 1:
                next_event = self.remove_event()
                if str(this_event) != str(next_event):
                    self.resume(next_event)
                    #print(threading.current_thread().name+" start pause if")
                    self.pause(this_event)
                    #print(threading.current_thread().name+" end pause if")
            else:
                #print(threading.current_thread().name+" start pause else")
                self.pause(this_event)
                #print(threading.current_thread().name+" end pause else")
            self.sim_time = this_event.get_time()
            #print(threading.current_thread().name+" end sleep "+str(self.sim_time))
            self.lock.release()

        else: # Real time
            time.sleep(delta)


#
# Set up the target
#

class PrintStdout:
    lock = threading.Lock()
    def print(self, record):
        with self.lock:
            print(str(record))
            sys.stdout.flush()
    def __str__(self):
        return '#printStdout()'

class PrintFile:
    f = None
    def __init__(self, file_name):
        self.file_name = file_name
        self.f = open(file_name, 'w')
    def __str__(self):
        return 'PrintFile(file_name='+self.file_name+')'
    def print(self, record):
        self.f.write(str(record)+'\n')
        self.f.flush()

class PrintKafka:
    producer = None
    topic = None
    def __init__(self, endpoint, topic, security_protocol, compression_type):
        #print('PrintKafka('+str(endpoint)+', '+str(topic)+', '+str(security_protocol)+', '+str(compression_type)+')')
        self.endpoint = endpoint
        self.producer = KafkaProducer(bootstrap_servers=endpoint, security_protocol=security_protocol, compression_type=compression_type, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.topic = topic
    def __str__(self):
        return 'PrintKafka(endpoint='+self.endpoint+', topic='+self.topic+')'
    def print(self, record):
        self.producer.send(self.topic, json.loads(str(record)))

class PrintConfluent:
    producer = None
    topic = None
    username = None
    password = None
    def __init__(self, servers, topic, username, password):
        #print('PrintKafka('+str(endpoint)+', '+str(topic)+', '+str(security_protocol)+', '+str(compression_type)+')')
        self.servers = servers
        self.producer = Producer({
            'bootstrap.servers': servers,
            'sasl.mechanisms': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': username,
            'sasl.password': password
        })
        self.topic = topic
        self.username = username
        self.password = password
    def __str__(self):
        return 'PrintConfluent(servers='+self.servers+', topic='+self.topic+', username='+self.username+', password='+self.password+')'
    def print(self, record):
        print('producing '+str(record))
        self.producer.produce(topic=self.topic, value=str(record))
        self.producer.flush()


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
        value = dateutil.parser.isoparse(desc['value']).timestamp()
        dist_gen = DistConstant(value)
    elif dist_type == 'uniform':
        min_value = dateutil.parser.isoparse(desc['min']).timestamp()
        max_value = dateutil.parser.isoparse(desc['max']).timestamp()
        dist_gen = DistUniform(min_value, max_value)
    elif dist_type == 'exponential':
        mean = dateutil.parser.isoparse(desc['mean']).timestamp()
        dist_gen = DistExponential(mean)
    elif dist_type == 'normal':
        mean = desc[dateutil.parser.isoparse(desc['mean']).timestamp()]
        stddev = desc['stddev']
        dist_gen = DistNormal(mean, stddev)
    else:
        print('Error: Unknown distribution "'+dist_type+'"')
        exit()
    return dist_gen


#
# Set up the dimensions for the emitters (see below)
# There is one element class for each dimension type. This code creates a list of
# elements and then runs through the list to create a single record.
# Notice that the get_json_field_string() method produces the JSON dimension
# field object based on the dimension configuration.
# The get_stochastic_value() method is like a private method used to get a random
# idividual value.
#

class ElementNow: # The __time dimension
    def __init__(self, global_clock):
        self.global_clock = global_clock
    def __str__(self):
        return 'ElementNow()'
    def get_json_field_string(self):
        now = self.global_clock.now().isoformat()[:-3]
        return '"__time":"'+now+'"'

class ElementCounter: # The __time dimension
    def __init__(self, desc):
        self.name = desc['name']
        if 'percent_nulls' in desc.keys():
            self.percent_nulls = desc['percent_nulls'] / 100.0
        else:
            self.percent_nulls = 0.0
        if 'percent_missing' in desc.keys():
            self.percent_missing = desc['percent_missing'] / 100.0
        else:
            self.percent_missing = 0.0
        if 'start' in desc.keys():
            self.start = desc['start']
        else:
            self.start = 0
        if 'increment' in desc.keys():
            self.increment = desc['increment']
        else:
            self.increment = 1
        self.value = self.start
    def __str__(self):
        s = 'ElementCounter(name='+self.name
        if self.start != 0:
            s += ', '+str(self.start)
        if self.increment != 1:
            s += ', '+str(self.increment)
        s += ')'
        return s

    def get_stochastic_value(self):
        v = self.value
        self.value += self.increment
        return v

    def get_json_field_string(self):
        if random.random() < self.percent_nulls:
            s = '"'+self.name+'": null'
        else:
            s = '"'+self.name+'":"'+str(self.get_stochastic_value())+'"'
            return s

    def is_missing(self):
        return random.random() < self.percent_missing


class ElementEnum: # enumeration dimensions
    def __init__(self, desc):
        self.name = desc['name']
        if 'percent_nulls' in desc.keys():
            self.percent_nulls = desc['percent_nulls'] / 100.0
        else:
            self.percent_nulls = 0.0
        if 'percent_missing' in desc.keys():
            self.percent_missing = desc['percent_missing'] / 100.0
        else:
            self.percent_missing = 0.0
        self.cardinality = desc['values']
        if 'cardinality_distribution' not in desc.keys():
            print('Element '+self.name+' specifies a cardinality without a cardinality distribution')
            exit()
        self.cardinality_distribution = parse_distribution(desc['cardinality_distribution'])

    def __str__(self):
        return 'ElementEnum(name='+self.name+', cardinality='+str(self.cardinality)+', cardinality_distribution='+str(self.cardinality_distribution)+')'

    def get_stochastic_value(self):
        index = int(self.cardinality_distribution.get_sample())
        if index < 0:
            index = 0
        if index >= len(self.cardinality):
            index = len(self.cardinality)-1
        return self.cardinality[index]

    def get_json_field_string(self):
        if random.random() < self.percent_nulls:
            s = '"'+self.name+'": null'
        else:
            s = '"'+self.name+'":"'+str(self.get_stochastic_value())+'"'
        return s

    def is_missing(self):
        return random.random() < self.percent_missing

class ElementVariable: # Variable dimensions
    def __init__(self, desc):
        self.name = desc['name']
        self.variable_name = desc['variable']

    def __str__(self):
        return 'ElementVariable(name='+self.name+', value='+self.variable_name+')'

    def get_json_field_string(self, variables): # NOTE: because of timing, this method has a different signature than the other elements
        value = variables[self.variable_name]
        return '"'+self.name+'":"'+str(value)+'"'


class ElementBase: # Base class for the remainder of the dimensions
    def __init__(self, desc):
        self.name = desc['name']
        if 'percent_nulls' in desc.keys():
            self.percent_nulls = desc['percent_nulls'] / 100.0
        else:
            self.percent_nulls = 0.0
        if 'percent_missing' in desc.keys():
            self.percent_missing = desc['percent_missing'] / 100.0
        else:
            self.percent_missing = 0.0
        cardinality = desc['cardinality']
        if cardinality == 0:
            self.cardinality = None
            self.cardinality_distribution = None
        else:
            self.cardinality = []
            if 'cardinality_distribution' not in desc.keys():
                print('Element '+self.name+' specifies a cardinality without a cardinality distribution')
                exit()
            self.cardinality_distribution = parse_distribution(desc['cardinality_distribution'])
            for i in range(cardinality):
                Value = None
                while True:
                    value = self.get_stochastic_value()
                    if value not in self.cardinality:
                        break
                self.cardinality.append(value)


    def get_stochastic_value(self):
        pass

    def get_json_field_string(self):
        if random.random() < self.percent_nulls:
            s = '"'+self.name+'": null'
        else:
            if self.cardinality is None:
                value = self.get_stochastic_value()
            else:
                index = int(self.cardinality_distribution.get_sample())
                if index < 0:
                    index = 0
                if index >= len(self.cardinality):
                    index = len(self.cardinality)-1
                value = self.cardinality[index]
            s = '"'+self.name+'":'+str(value)
        return s

    def is_missing(self):
        return random.random() < self.percent_missing


class ElementString(ElementBase):
    def __init__(self, desc):
        self.length_distribution = parse_distribution(desc['length_distribution'])
        if 'chars' in desc:
            self.chars = desc['chars']
        else:
            self.chars = string.printable
        super().__init__(desc)

    def __str__(self):
        return 'ElementString(name='+self.name+', cardinality='+str(self.cardinality)+', cardinality_distribution='+str(self.cardinality_distribution)+', chars='+self.chars+')'

    def get_stochastic_value(self):
        length = int(self.length_distribution.get_sample())
        return ''.join(random.choices(list(self.chars), k=length))

    def get_json_field_string(self):
        if random.random() < self.percent_nulls:
            s = '"'+self.name+'": null'
        else:
            if self.cardinality is None:
                value = self.get_stochastic_value()
            else:
                index = int(self.cardinality_distribution.get_sample())
                if index < 0:
                    index = 0
                if index >= len(self.cardinality):
                    index = len(self.cardinality)-1
                value = self.cardinality[index]
            s = '"'+self.name+'":"'+str(value)+'"'
        return s

class ElementInt(ElementBase):
    def __init__(self, desc):
        self.value_distribution = parse_distribution(desc['distribution'])
        super().__init__(desc)

    def __str__(self):
        return 'ElementInt(name='+self.name+', value_distribution='+str(self.value_distribution)+', cardinality='+str(self.cardinality)+', cardinality_distribution='+str(self.cardinality_distribution)+')'

    def get_stochastic_value(self):
        return int(self.value_distribution.get_sample())

class ElementFloat(ElementBase):
    def __init__(self, desc):
        self.value_distribution = parse_distribution(desc['distribution'])
        if 'precision' in desc:
            self.precision = desc['precision']
        else:
            self.precision = None
        super().__init__(desc)

    def __str__(self):
        return 'ElementFloat(name='+self.name+', value_distribution='+str(self.value_distribution)+', cardinality='+str(self.cardinality)+', cardinality_distribution='+str(self.cardinality_distribution)+')'

    def get_stochastic_value(self):
        return float(self.value_distribution.get_sample())

    def get_json_field_string(self):
        if random.random() < self.percent_nulls:
            s = '"'+self.name+'": null'
        else:
            if self.cardinality is None:
                value = self.get_stochastic_value()
            else:
                index = int(self.cardinality_distribution.get_sample())
                if index < 0:
                    index = 0
                if index >= len(self.cardinality):
                    index = len(self.cardinality)-1
                value = self.cardinality[index]
            if self.precision is None:
                s = '"'+self.name+'":'+str(value)
            else:
                format = '%.'+str(self.precision)+'f'
                s = '"'+self.name+'":'+str(format%value)
        return s

class ElementTimestamp(ElementBase):
    def __init__(self, desc):
        self.name = desc['name']
        self.value_distribution = parse_timestamp_distribution(desc['distribution'])
        if 'percent_nulls' in desc.keys():
            self.percent_nulls = desc['percent_nulls'] / 100.0
        else:
            self.percent_nulls = 0.0
        if 'percent_missing' in desc.keys():
            self.percent_missing = desc['percent_missing'] / 100.0
        else:
            self.percent_missing = 0.0
        cardinality = desc['cardinality']
        if cardinality == 0:
            self.cardinality = None
            self.cardinality_distribution = None
        else:
            if 'cardinality_distribution' not in desc.keys():
                print('Element '+self.name+' specifies a cardinality without a cardinality distribution')
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
        return 'ElementTimestamp(name='+self.name+', value_distribution='+str(self.value_distribution)+', cardinality='+str(self.cardinality)+', cardinality_distribution='+str(self.cardinality_distribution)+')'

    def get_stochastic_value(self):
        return datetime.fromtimestamp(self.value_distribution.get_sample()).isoformat()[:-3]

    def get_json_field_string(self):
        if random.random() < self.percent_nulls:
            s = '"'+self.name+'": null'
        else:
            if self.cardinality is None:
                value = self.get_stochastic_value()
            else:
                index = int(self.cardinality_distribution.get_sample())
                if index < 0:
                    index = 0
                if index >= len(self.cardinality):
                    index = len(self.cardinality)-1
                value = self.cardinality[index]
            s = '"'+self.name+'":"'+str(value)+'"'
        return s

    def is_missing(self):
        return random.random() < self.percent_missing

class ElementIPAddress(ElementBase):
    def __init__(self, desc):
        self.value_distribution = parse_distribution(desc['distribution'])
        super().__init__(desc)

    def __str__(self):
        return 'ElementIPAddress(name='+self.name+', value_distribution='+str(self.value_distribution)+', cardinality='+str(self.cardinality)+', cardinality_distribution='+str(self.cardinality_distribution)+')'

    def get_stochastic_value(self):
        value = int(self.value_distribution.get_sample())
        return str((value & 0xFF000000) >> 24)+'.'+str((value & 0x00FF0000) >> 16)+'.'+str((value & 0x0000FF00) >> 8)+'.'+str(value & 0x000000FF)

    def get_json_field_string(self):
        if random.random() < self.percent_nulls:
            s = '"'+self.name+'": null'
        else:
            if self.cardinality is None:
                value = self.get_stochastic_value()
            else:
                index = int(self.cardinality_distribution.get_sample())
                if index < 0:
                    index = 0
                if index >= len(self.cardinality):
                    index = len(self.cardinality)-1
                value = self.cardinality[index]
            s = '"'+self.name+'":"'+str(value)+'"'
        return s

class ElementObject():
    def __init__(self, desc):
        self.name = desc['name']
        self.dimensions = get_variables(desc['dimensions'])
        if 'percent_nulls' in desc.keys():
            self.percent_nulls = desc['percent_nulls'] / 100.0
        else:
            self.percent_nulls = 0.0
        if 'percent_missing' in desc.keys():
            self.percent_missing = desc['percent_missing'] / 100.0
        else:
            self.percent_missing = 0.0
        cardinality = desc['cardinality']
        if cardinality == 0:
            self.cardinality = None
            self.cardinality_distribution = None
        else:
            self.cardinality = []
            if 'cardinality_distribution' not in desc.keys():
                print('Element '+self.name+' specifies a cardinality without a cardinality distribution')
                exit()
            self.cardinality_distribution = parse_distribution(desc['cardinality_distribution'])
            for i in range(cardinality):
                Value = None
                while True:
                    value = self.get_instance()
                    if value not in self.cardinality:
                        break
                self.cardinality.append(value)

    def __str__(self):
        s = 'ElementObject(name='+self.name+', dimensions=['
        for e in self.dimensions:
            s += ',' + str(e)
        s += '])'
        return s

    def get_instance(self):
        s = '"'+self.name+'": {'
        for e in self.dimensions:
            s += e.get_json_field_string() + ','
        s = s[:-1] +  '}'
        return s


    def get_json_field_string(self):
        if random.random() < self.percent_nulls:
            s = '"'+self.name+'": null'
        else:
            if self.cardinality is None:
                s = self.get_instance()
            else:
                index = int(self.cardinality_distribution.get_sample())
                if index < 0:
                    index = 0
                if index >= len(self.cardinality):
                    index = len(self.cardinality)-1
                s = self.cardinality[index]
        return s

    def is_missing(self):
        return random.random() < self.percent_missing

class ElementList():
    def __init__(self, desc):
        self.name = desc['name']
        self.elements = get_variables(desc['elements'])
        self.length_distribution = parse_distribution(desc['length_distribution'])
        self.selection_distribution = parse_distribution(desc['selection_distribution'])
        if 'percent_nulls' in desc.keys():
            self.percent_nulls = desc['percent_nulls'] / 100.0
        else:
            self.percent_nulls = 0.0
        if 'percent_missing' in desc.keys():
            self.percent_missing = desc['percent_missing'] / 100.0
        else:
            self.percent_missing = 0.0
        cardinality = desc['cardinality']
        if cardinality == 0:
            self.cardinality = None
            self.cardinality_distribution = None
        else:
            self.cardinality = []
            if 'cardinality_distribution' not in desc.keys():
                print('Element '+self.name+' specifies a cardinality without a cardinality distribution')
                exit()
            self.cardinality_distribution = parse_distribution(desc['cardinality_distribution'])
            for i in range(cardinality):
                Value = None
                while True:
                    value = self.get_instance()
                    if value not in self.cardinality:
                        break
                self.cardinality.append(value)

    def __str__(self):
        s = 'ElementObject(name='+self.name
        s += ', length_distribution='+str(self.length_distribution)
        s += ', selection_distribution='+str(self.selection_distribution)
        s += ', elements=['
        for e in self.elements:
            s += ',' + str(e)
        s += '])'
        return s

    def get_instance(self):
        s = '"'+self.name+'": ['
        length = int(self.length_distribution.get_sample())
        for i in range(length):
            index = int(self.selection_distribution.get_sample())
            if index < 0:
                index = 0
            if index >= length:
                index = length-1
            s += re.sub('^.*?:', '', self.elements[index].get_json_field_string(), count=1) + ','
        s = s[:-1] +  ']'
        return s


    def get_json_field_string(self):
        if random.random() < self.percent_nulls:
            s = '"'+self.name+'": null'
        else:
            if self.cardinality is None:
                s = self.get_instance()
            else:
                index = int(self.cardinality_distribution.get_sample())
                if index < 0:
                    index = 0
                if index >= len(self.cardinality):
                    index = len(self.cardinality)-1
                s = self.cardinality[index]
        return s

    def is_missing(self):
        return random.random() < self.percent_missing


def parse_element(desc):
    if desc['type'].lower() == 'counter':
        el = ElementCounter(desc)
    elif desc['type'].lower() == 'enum':
        el = ElementEnum(desc)
    elif desc['type'].lower() == 'string':
        el = ElementString(desc)
    elif desc['type'].lower() == 'int':
        el = ElementInt(desc)
    elif desc['type'].lower() == 'float':
        el = ElementFloat(desc)
    elif desc['type'].lower() == 'timestamp':
        el = ElementTimestamp(desc)
    elif desc['type'].lower() == 'ipaddress':
        el = ElementIPAddress(desc)
    elif desc['type'].lower() == 'variable':
        el = ElementVariable(desc)
    elif desc['type'].lower() == 'object':
        el = ElementObject(desc)
    elif desc['type'].lower() == 'list':
        el = ElementList(desc)
    else:
        print('Error: Unknown dimension type "'+desc['type']+'"')
        exit()
    return el


def get_variables(desc):
    elements = []
    for element in desc:
        el = parse_element(element)
        elements.append(el)
    return elements

def get_dimensions(desc, global_clock):
    elements = get_variables(desc)
    elements.insert(0, ElementNow(global_clock))
    return elements


#
# Set up the state machine
#

class Transition:
    def __init__(self, next_state, probability):
        self.next_state = next_state
        self.probability = probability

    def __str__(self):
        return 'Transition(next_state='+str(self.next_state)+', probability='+str(self.probability)+')'

def parse_transitions(desc):
    transitions = []
    for trans in desc:
        next_state = trans['next']
        probability = float(trans['probability'])
        transitions.append(Transition(next_state, probability))
    return transitions

class State:
    def __init__(self, name, dimensions, delay, transitions, variables):
        self.name = name
        self.dimensions = dimensions
        self.delay = delay
        self.transistion_states = [t.next_state for t in transitions]
        self.transistion_probabilities = [t.probability for t in transitions]
        self.variables = variables

    def __str__(self):
        return 'State(name='+self.name+', dimensions='+str([str(d) for d in self.dimensions])+', delay='+str(self.delay)+', transistion_states='+str(self.transistion_states)+', transistion_probabilities='+str(self.transistion_probabilities)+'variables='+str([str(v) for v in self.variables])+')'

    def get_next_state_name(self):
        return random.choices(self.transistion_states, weights=self.transistion_probabilities, k=1)[0]

class SimEnd:
    lock = threading.Lock()
    thread_end_event = threading.Event()
    def __init__(self, total_recs, runtime, global_clock):
        self.total_recs = total_recs
        self.record_count = 0
        self.global_clock = global_clock
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

    def inc_rec_count(self):
        self.lock.acquire()
        self.record_count += 1
        self.lock.release()
        if (self.total_recs is not None) and (self.record_count >= self.total_recs):
            self.thread_end_event.set()

    def is_done(self):
        return ((self.total_recs is not None) and (self.record_count >= self.total_recs)) or ((self.t is not None) and self.thread_end_event.is_set())

    def wait_for_end(self):
        if self.t is not None:
            self.global_clock.activate_thread()
            self.global_clock.sleep(self.t)
            self.thread_end_event.set()
            self.global_clock.deactivate_thread()
        elif self.total_recs is not None:
            self.thread_end_event.wait()
            self.global_clock.release_all()
        else:
            while True:
                time.sleep(60)


#
# Run the driver
#

def create_record(dimensions, variables):
    json_string = '{'
    for element in dimensions:
        if isinstance(element, ElementVariable):
            json_string += element.get_json_field_string(variables) + ','
        else:
            if isinstance(element, ElementNow) or not element.is_missing():
                json_string += element.get_json_field_string() + ','
    json_string = json_string[:-1] + '}'
    return json_string

def set_variable_values(variables, dimensions):
    for d in dimensions:
        variables[d.name] = d.get_stochastic_value()

def worker_thread(target_printer, states, initial_state, sim_end, global_clock):
    # Process the state machine using worker threads
    #print('Thread '+threading.current_thread().name+' starting...')
    global_clock.activate_thread()
    current_state = initial_state
    variables = {}
    while True:
        set_variable_values(variables, current_state.variables)
        record = create_record(current_state.dimensions, variables)
        target_printer.print(record)
        sim_end.inc_rec_count()
        if sim_end.is_done():
            break
        delta = float(current_state.delay.get_sample())
        global_clock.sleep(delta)
        if sim_end.is_done():
            break
        next_state_name = current_state.get_next_state_name()
        if next_state_name.lower() == 'stop':
            break
        current_state = states[next_state_name]

    #print('Thread '+threading.current_thread().name+' done!')
    global_clock.end_thread()

def spawning_thread(target_printer, rate_delay, states, initial_state, sim_end, global_clock):
    #print('Thread '+threading.current_thread().name+' starting...')
    global_clock.activate_thread()
    # Spawn the workers in a separate thread so we can stop the whole thing in the middle of spawning if necessary
    count = 0
    while not sim_end.is_done():
        thread_name = 'W'+str(count)

        count += 1
        t = threading.Thread(target=worker_thread, args=(target_printer, states, initial_state, sim_end, global_clock, ), name=thread_name, daemon=True)
        t.start()
        global_clock.sleep(float(rate_delay.get_sample()))
    global_clock.end_thread()
    #print('Thread '+threading.current_thread().name+' done!')


def simulate(config_file_name, runtime, total_recs, time_type, start_time):

    if config_file_name:
        with open(config_file_name, 'r') as f:
            config = json.load(f)
    else:
        config = json.load(sys.stdin)

    #
    # Set up the gloabl clock
    #

    global_clock = Clock(time_type, start_time)
    sim_end = SimEnd(total_recs, runtime, global_clock)


    #
    # Set up the output target
    #

    target = config['target']

    if target['type'].lower() == 'stdout':
        target_printer = PrintStdout()
    elif target['type'].lower() == 'file':
        path = target['path']
        if path is None:
            print('Error: File target requires a path item')
            exit()
        target_printer = PrintFile(path)
    elif target['type'].lower() == 'kafka':
        if 'endpoint' in target.keys():
            endpoint = target['endpoint']
        else:
            print('Error: Kafka target requires an endpoint item')
            exit()
        if 'topic' in target.keys():
            topic = target['topic']
        else:
            print('Error: Kafka target requires a topic item')
            exit()
        if 'security_protocol' in target.keys():
            security_protocol = target['security_protocol']
        else:
            security_protocol = 'PLAINTEXT'
        if 'compression_type' in target.keys():
            compression_type = target['compression_type']
        else:
            compression_type = None
        target_printer = PrintKafka(endpoint, topic, security_protocol, compression_type)
    elif target['type'].lower() == 'confluent':
        if 'servers' in target.keys():
            servers = target['servers']
        else:
            print('Error: Conlfuent target requires a servers item')
            exit()
        if 'topic' in target.keys():
            topic = target['topic']
        else:
            print('Error: Confluent target requires a topic item')
            exit()
        if 'username' in target.keys():
            username = target['username']
        else:
            print('Error: Confluent target requires a username')
            exit()
        if 'password' in target.keys():
            password = target['password']
        else:
            print('Error: Confluent target requires a password')
            exit()
        target_printer = PrintConfluent(servers, topic, username, password)
    else:
        print('Error: Unknown target type "'+target['type']+'"')
        exit()

    #sys.stderr.write('target='+str(target_printer)+'\n')


    #
    # Set up the interarrival rate
    #

    rate = config['interarrival']
    rate_delay = parse_distribution(rate)

    #sys.stderr.write('rate_delay='+str(rate_delay)+'\n')


    #
    # Set up emitters list
    #

    emitters = {}
    for emitter in config['emitters']:
        name = emitter['name']
        dimensions = get_dimensions(emitter['dimensions'], global_clock)
        emitters[name] = dimensions

    #sys.stderr.write('emitters='+str(['(name='+str(key)+', dimensions='+str([str(e) for e in emitters[key]])+')' for key in emitters])+'\n')


    #
    # Set up the state machine
    #

    state_desc = config['states']
    initial_state = None
    states = {}
    for state in state_desc:
        name = state['name']
        emitter_name = state['emitter']
        if 'variables' not in state.keys():
            variables = []
        else:
            variables = get_variables(state['variables'])
        dimensions = emitters[emitter_name]
        delay = parse_distribution(state['delay'])
        transitions = parse_transitions(state['transitions'])
        this_state = State(name, dimensions, delay, transitions, variables)
        states[name] = this_state
        if initial_state == None:
            initial_state = this_state

    #sys.stderr.write('states='+str(['('+str(key)+':'+str(states[key])+')' for key in states])+'\n')

    #
    # Finally, start the simulation
    #

    thrd = threading.Thread(target=spawning_thread, args=(target_printer, rate_delay, states, initial_state, sim_end, global_clock, ), name='Spawning', daemon=True)
    thrd.start()
    sim_end.wait_for_end()

def main():

    #
    # Parse the command line
    #

    parser = argparse.ArgumentParser(description='Generates JSON records as a workload for Apache Druid.')
    #parser.add_argument('config_file', metavar='<config file name>', help='the workload config file name')
    parser.add_argument('-f', dest='config_file', nargs='?', help='the workload config file name')
    parser.add_argument('-t', dest='time', nargs='?', help='the script runtime (may not be used with -n)')
    parser.add_argument('-n', dest='n_recs', nargs='?', help='the number of records to generate (may not be used with -t)')
    parser.add_argument('-s', dest='time_type', nargs='?', const='SIM', default='REAL', help='simulate time (default is real, not simulated)')

    args = parser.parse_args()

    config_file_name = args.config_file
    runtime = args.time
    total_recs = None
    if args.n_recs is not None:
        total_recs = int(args.n_recs)
    time_type = args.time_type
    if time_type == 'SIM':
        start_time = datetime.now()
    elif time_type == 'REAL':
        start_time = datetime.now()
    else:
        start_time = dateutil.parser.isoparse(time_type)
        time_type = 'SIM'


    if (runtime is not None) and (total_recs is not None):
        print("Use either -t or -n, but not both")
        parser.print_help()
        exit()


    simulate(config_file_name, runtime, total_recs, time_type, start_time)



if __name__ == "__main__":
    main()
