# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys
import os
import multiprocessing
import argparse
import subprocess
import platform

BASE_CONFIG_PATH = "conf/druid/auto"

MEM_GB_SUFFIX = "g"
MEM_MB_SUFFIX = "m"
XMX_PARAMETER = "-Xmx"
XMS_PARAMETER = "-Xms"
DIRECT_MEM_PARAMETER = "-XX:MaxDirectMemorySize"
SERVICE_SEPARATOR = ","

TASK_JAVA_OPTS_ARRAY = ["-server", "-Duser.timezone=UTC", "-Dfile.encoding=UTF-8", "-XX:+ExitOnOutOfMemoryError",
                        "-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager"]
TASK_JAVA_OPTS_PROPERTY = "druid.indexer.runner.javaOptsArray"
TASK_WORKER_CAPACITY_PROPERTY = "druid.worker.capacity"
TASK_COUNT = "task-count"

BROKER = "broker"
ROUTER = "router"
COORDINATOR = "coordinator-overlord"
HISTORICAL = "historical"
MIDDLE_MANAGER = "middleManager"
TASKS = "tasks"
INDEXER = "indexer"
ZK = "zookeeper"

DEFAULT_SERVICES = [
    BROKER,
    ROUTER,
    COORDINATOR,
    HISTORICAL,
    MIDDLE_MANAGER
]

SUPPORTED_SERVICES = [
    BROKER,
    ROUTER,
    COORDINATOR,
    HISTORICAL,
    MIDDLE_MANAGER,
    INDEXER,
    ZK
]

SERVICE_MEMORY_RATIO = {
    MIDDLE_MANAGER: 1,
    ROUTER: 2,
    COORDINATOR: 30,
    BROKER: 46,
    HISTORICAL: 80,
    TASKS: 30,
    INDEXER: 32
}

MINIMUM_MEMORY_MB = {
    MIDDLE_MANAGER: 64,
    ROUTER: 256,
    TASKS: 1024,
    BROKER: 900,
    COORDINATOR: 256,
    HISTORICAL: 900,
    INDEXER: 1124
}

HEAP_TO_TOTAL_MEM_RATIO = {
    MIDDLE_MANAGER: 1,
    ROUTER: 1,
    COORDINATOR: 1,
    BROKER: 0.60,
    HISTORICAL: 0.40,
    TASKS: 0.50,
    INDEXER: 0.50
}

LOGGING_ENABLED = False


def print_if_verbose(message):
    if LOGGING_ENABLED:
        print(message)


def configure_parser():
    parser = argparse.ArgumentParser(
        prog='start-druid',
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=
        """
sample usage:
    start-druid
            Start up all the services (including zk).
            services config is read from conf/druid/auto.
            zk config is always read from conf/zk.
    start-druid -m=100g
            Start up all the services (including zk)
            using a total memory of 100GB.
    start-druid -m=100g --compute
            Compute memory distribution and validate arguments.
    start-druid -m=100g -s=broker,router
            Starts a broker and a router, using a total memory of 100GB.
    start-druid -m=100g --s=broker,router \\
    -c=conf/druid/single-server/custom
            Starts a broker and a router, using a total memory of 100GB.
            Reads configs for each service (jvm.config, runtime.properties)
            from respective folders inside the given root config path.
    start-druid -s=broker,router \\
    -c=conf/druid/single-server/custom
            Starts a broker and a router service, reading service configs
            from the given root directory. Calculates memory requirements for
            each service, if required, using upto 80% of the total system memory.
    start-druid -m=100g \\
    -s=broker,router,zookeeper \\
    -c=conf/druid/single-server/custom \\
            Starts broker, router and zookeeper.
            Configs for broker and router are read from the specified root directory.
            Config for zookeeper is read from conf/zk.
"""
    )
    parser.add_argument('--memory', '-m', type=str, required=False,
                        help='Total memory for all processes (services and tasks, if any). \n'
                             'This parameter is ignored if each service already has a jvm.config \n'
                             'in the given conf directory. e.g. 500m, 4g, 6g\n')
    parser.add_argument('--services', '-s', type=str, required=False,
                        help='List of services to be started, subset of \n'
                             '{broker, router, middleManager, historical, coordinator-overlord, indexer, zookeeper}. \n'
                             'If the argument is not given, broker, router, middleManager, historical, coordinator-overlord  \n'
                             'and zookeeper is started. e.g. -s=broker,historical')
    parser.add_argument('--config', '-c', type=str, required=False,
                        help='Relative path to the directory containing common and service \n'
                             'specific properties to be overridden. \n'
                             'This directory must contain \'_common\' directory with \n'
                             '\'common.jvm.config\' & \'common.runtime.properties\' files. \n'
                             'If this argument is not given, config from \n'
                             'conf/druid/auto directory is used.\n'
                             'Note. zookeeper config cannot be overridden.\n')
    parser.add_argument('--compute', action='store_true',
                        help='Does not start Druid, only displays the memory allocated \n'
                             'to each service if started with the given total memory.\n')
    parser.add_argument('--verbose', action='store_true', help='Log details')

    parser.set_defaults(compute=False)
    parser.set_defaults(verbose=False)

    return parser


def is_file(path):
    return os.path.isfile(path)


def is_dir(path):
    return os.path.isdir(path)


def resolve_path(path):
    return os.path.abspath(path)


def validate_common_jvm_args(config):
    if is_file('{0}/_common/common.jvm.config'.format(config)) is False:
        raise ValueError('_common/common.jvm.config file is missing in the root config, '
                         'check {0}/_common directory'.format(BASE_CONFIG_PATH))


def validate_common_directory(config):
    if is_dir('{0}/_common'.format(config)) is False:
        raise ValueError(
            '_common directory is missing in the root config, check {0}/_common directory'.format(BASE_CONFIG_PATH))

    if is_file('{0}/_common/common.runtime.properties'.format(config)) is False:
        raise ValueError('_common/common.runtime.properties file is missing in the root config, '
                         'check {0}/_common directory'.format(BASE_CONFIG_PATH))


def parse_arguments(args):
    service_list = []
    config = ""
    total_memory = ""
    compute = False
    zk = False

    if args.compute:
        compute = True
    if args.config is not None:
        config = resolve_path(os.path.join(os.getcwd(), args.config))
        if is_dir(config) is False:
            raise ValueError('config {0} not found'.format(config))
    if args.memory is not None:
        total_memory = args.memory
    if args.services is not None:
        services = args.services.split(SERVICE_SEPARATOR)

        for service in services:
            if service not in SUPPORTED_SERVICES:
                raise ValueError('Invalid service name {0}, should be one of {1}'.format(service, SUPPORTED_SERVICES))

            if service in service_list:
                raise ValueError('{0} is specified multiple times'.format(service))

            if service == ZK:
                zk = True
                continue

            service_list.append(service)

        if INDEXER in services and MIDDLE_MANAGER in services:
            raise ValueError('one of indexer and middleManager can run')


    if len(service_list) == 0:
        # start all services
        service_list = DEFAULT_SERVICES
        zk = True

    return config, total_memory, service_list, zk, compute


def print_startup_config(service_list, config, zk):
    print_if_verbose('Starting {0}'.format(service_list))
    print_if_verbose('Reading config from {0}'.format(config))
    if zk:
        zk_config = resolve_path('{0}/../conf/zk'.format(os.getcwd()))
        print_if_verbose('Starting zk, reading default config from {0}'.format(zk_config))
    print_if_verbose('\n')


def task_memory_params_present(config, service):
    java_opts_property_present = False
    worker_capacity_property_present = False

    if is_file('{0}/{1}/runtime.properties'.format(config, service)):
        with open('{0}/{1}/runtime.properties'.format(config, service)) as file:
            for line in file:
                if line.startswith(TASK_JAVA_OPTS_PROPERTY):
                    java_opts_property_present = True
                elif line.startswith(TASK_WORKER_CAPACITY_PROPERTY):
                    worker_capacity_property_present = True

    return java_opts_property_present, worker_capacity_property_present


def verify_service_config(service, config):
    path = '{0}/{1}/jvm.config'.format(config, service)

    required_parameters = [XMX_PARAMETER, XMS_PARAMETER]

    if HEAP_TO_TOTAL_MEM_RATIO.get(service) != 1:
        required_parameters.append(DIRECT_MEM_PARAMETER)

    with open(path) as file:
        for line in file:
            if line.startswith(XMX_PARAMETER) and XMX_PARAMETER in required_parameters:
                required_parameters.remove(XMX_PARAMETER)
            if line.startswith(XMS_PARAMETER) and XMS_PARAMETER in required_parameters:
                required_parameters.remove(XMS_PARAMETER)
            if line.startswith(DIRECT_MEM_PARAMETER) and DIRECT_MEM_PARAMETER in required_parameters:
                required_parameters.remove(DIRECT_MEM_PARAMETER)

    if len(required_parameters) > 0:
        params = ",".join(required_parameters)
        raise ValueError('{0} missing in {1}/jvm.config'.format(params, service))

    if service == MIDDLE_MANAGER:
        if is_file('{0}/{1}/runtime.properties'.format(config, service)) is False:
            raise ValueError('{0}/runtime.properties file is missing in the root config'.format(service))

        mm_task_java_opts_prop, mm_task_worker_capacity_prop = task_memory_params_present(config, MIDDLE_MANAGER)

        if mm_task_java_opts_prop is False:
            raise ValueError('{0} property missing in {1}/runtime.properties'.format(TASK_JAVA_OPTS_PROPERTY, service))


def should_compute_memory(config, total_memory, service_list):
    """
    if memory argument is given, memory for services and tasks is computed, jvm.config file
    or runtime.properties with task memory specification shouldn't be present
    Alternatively, all memory related parameters are specified
    which implies following should be present:
    jvm.config file for all services with -Xmx=***, Xms=*** parameters
    -XX:MaxDirectMemorySize=** in jvm.config for broker and historical
    druid.indexer.runner.javaOptsArray (optionally druid.worker.capacity) in
    rootDirectory/middleManager/runtime.properties
    """

    jvm_config_count = 0
    for service in service_list:
        if is_file('{0}/{1}/jvm.config'.format(config, service)):
            jvm_config_count += 1

    mm_task_property_present = False
    if MIDDLE_MANAGER in service_list:
        mm_task_java_opts_prop, mm_task_worker_capacity_prop = task_memory_params_present(config, MIDDLE_MANAGER)
        mm_task_property_present = mm_task_java_opts_prop or mm_task_worker_capacity_prop

    indexer_task_worker_capacity_prop = False
    if INDEXER in service_list:
        indexer_task_java_opts_prop, indexer_task_worker_capacity_prop = task_memory_params_present(config, INDEXER)

    # possible error states
    # 1. memory argument is specified, also jvm.config or middleManger/runtime.properties having
    # druid.indexer.runner.javaOptsArray or druid.worker.capacity parameters is present
    # 2. jvm.config is not present for any service, but middleManger/runtime.properties has
    # druid.indexer.runner.javaOptsArray or druid.worker.capacity parameters
    # or indexer/runtime.properties has druid.worker.capacity
    # 3. jvm.config present for some but not all services
    # 4. jvm.config file is present for all services, but it doesn't contain required parameters
    # 5. lastly, if middleManager is to be started, and it is missing task memory properties
    if jvm_config_count > 0 or mm_task_property_present or indexer_task_worker_capacity_prop:
        if total_memory != "":
            raise ValueError(
                "If jvm.config for services and/or middleManager/indexer configs "
                "(druid.worker.capacity, druid.indexer.runner.javaOptsArray) is present, "
                "memory argument shouldn't be specified")
        if jvm_config_count == 0 and mm_task_property_present:
            raise ValueError("middleManger configs (druid.indexer.runner.javaOptsArray or druid.worker.capacity) "
                             "is present in middleManager/runtime.properties, "
                             "add jvm.config for all other services")
        if jvm_config_count == 0 and indexer_task_worker_capacity_prop:
            raise ValueError("indexer configs (druid.worker.capacity) "
                             "is present in indexer/runtime.properties, "
                             "add jvm.config for all other services")
        if jvm_config_count != len(service_list):
            raise ValueError("jvm.config file should be present for all services or none")
        for service in service_list:
            verify_service_config(service, config)

        return False

    # compute memory only when none of the specified services contains jvm.config,
    # if middleManager is to be started it shouldn't contain task memory properties
    # if indexer is present it shouldn't contain task memory properties
    return True


def get_physical_memory_linux():
    mem_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    mem_mbs = int(mem_bytes / (1024 * 1024))
    return mem_mbs


def get_physical_memory_osx():
    p1 = subprocess.Popen(['sysctl', '-a'], stdout=subprocess.PIPE)
    p2 = subprocess.check_output(['grep', 'hw.memsize:'], stdin=p1.stdout)
    p2 = p2.decode('utf-8')
    fields = p2.split(':')

    mem_mbs = int(int(fields[1]) / (1024 * 1024))

    return mem_mbs


def get_physical_memory():
    operating_system = platform.system()
    print_if_verbose('operating system is {0}'.format(operating_system))

    system_memory = None

    try:
        if operating_system == 'Darwin':
            system_memory = get_physical_memory_osx()
        elif operating_system == 'Linux':
            system_memory = get_physical_memory_linux()
    except Exception:
        pass

    return system_memory


def convert_total_memory_string(memory):
    try:
        if memory == '':
            physical_memory = get_physical_memory()

            if physical_memory is None:
                raise ValueError('Could not automatically determine memory size. Please explicitly specify the memory argument as --memory=<integer_value><m/g>')

            return physical_memory
        elif memory.endswith(MEM_MB_SUFFIX):
            return int(memory[:-1])
        elif memory.endswith(MEM_GB_SUFFIX):
            return 1024 * int(memory[:-1])
        else:
            raise ValueError('Incorrect format for memory argument, expected format is <integer_value><m/g>')
    except ValueError as e:
        raise e
    except Exception:
        raise ValueError('Incorrect format for memory argument, expected format is <integer_value><m/g>')


def check_memory_constraint(total_memory, services):
    # 80% of total memory >= sum of lower bound service memory should be
    lower_bound_memory = 0

    service_list = list(services)
    if MIDDLE_MANAGER in services:
        service_list.append(TASKS)

    for service in service_list:
        lower_bound_memory += MINIMUM_MEMORY_MB.get(service)

    required_memory = int(lower_bound_memory / 0.8)

    if total_memory < required_memory:
        raise ValueError('Minimum memory required for starting services is {0}m'.format(required_memory))

    return int(total_memory * 0.8)


def build_mm_task_java_opts_array(task_memory):
    memory = int(task_memory / 2)
    mem_array = ["-Xms{0}m".format(memory), "-Xmx{0}m".format(memory), "-XX:MaxDirectMemorySize={0}m".format(memory)]

    java_opts_list = TASK_JAVA_OPTS_ARRAY + mem_array

    task_java_opts_value = ''

    for item in java_opts_list:
        task_java_opts_value += '\"{0}\",'.format(item)

    task_java_opts_value = task_java_opts_value[:-1]
    task_memory_config = '-D{0}=[{1}]'.format(TASK_JAVA_OPTS_PROPERTY, task_java_opts_value)

    return task_memory_config


def compute_tasks_memory(allocated_memory):
    cpu_count = multiprocessing.cpu_count()

    if allocated_memory >= cpu_count * 1024:
        task_count = cpu_count
        task_memory_mb = min(2048, int(allocated_memory / cpu_count))
    elif allocated_memory >= 2048:
        task_count = int(allocated_memory / 1024)
        task_memory_mb = 1024
    else:
        task_count = 2
        task_memory_mb = int(allocated_memory / task_count)

    return task_count, task_memory_mb


def build_memory_config(service, allocated_memory):
    if service == TASKS:
        task_count, task_memory = compute_tasks_memory(allocated_memory)
        java_opts_array = build_mm_task_java_opts_array(task_memory)
        return ['-D{0}={1}'.format(TASK_WORKER_CAPACITY_PROPERTY, task_count),
                java_opts_array], task_memory * task_count
    elif service == INDEXER:
        heap_memory = HEAP_TO_TOTAL_MEM_RATIO.get(service) * allocated_memory
        direct_memory = int(allocated_memory - heap_memory)
        heap_memory = int(heap_memory)
        task_count, task_memory = compute_tasks_memory(allocated_memory)
        return ['-D{0}={1}'.format(TASK_WORKER_CAPACITY_PROPERTY, task_count),
                '-Xms{0}m -Xmx{0}m -XX:MaxDirectMemorySize={1}m'.format(heap_memory, direct_memory)], \
               task_memory * task_count
    else:
        heap_memory = HEAP_TO_TOTAL_MEM_RATIO.get(service) * allocated_memory
        direct_memory = int(allocated_memory - heap_memory)
        if service == ROUTER:
            direct_memory = 128
        heap_memory = int(heap_memory)

        if direct_memory == 0:
            return '-Xms{0}m -Xmx{0}m'.format(heap_memory), allocated_memory

        return '-Xms{0}m -Xmx{0}m -XX:MaxDirectMemorySize={1}m'.format(heap_memory, direct_memory), allocated_memory


def distribute_memory(services, total_memory):
    service_memory_config = {}

    memory_weight_sum = 0

    service_list = list(services)
    if MIDDLE_MANAGER in services:
        service_list.append(TASKS)

    for service in service_list:
        memory_weight_sum += SERVICE_MEMORY_RATIO.get(service)

    multiplier = total_memory / memory_weight_sum

    lower_bound_memory_allocation = 0
    allocated_services = set()

    for service in service_list:
        allocated_memory = SERVICE_MEMORY_RATIO.get(service) * multiplier
        if service in MINIMUM_MEMORY_MB and allocated_memory < MINIMUM_MEMORY_MB.get(service):
            allocated_memory = MINIMUM_MEMORY_MB.get(service)
            service_memory_config[service], allocated_memory = build_memory_config(service, allocated_memory)
            lower_bound_memory_allocation += allocated_memory
            allocated_services.add(service)

    if lower_bound_memory_allocation > 0:
        # compute the multiplier again for remaining services
        memory_weight_sum = 0
        for service in service_list:
            if service in allocated_services:
                continue
            memory_weight_sum += SERVICE_MEMORY_RATIO.get(service)
        multiplier = (total_memory - lower_bound_memory_allocation) / memory_weight_sum

    for service in service_list:
        if service in allocated_services:
            continue
        allocated_memory = SERVICE_MEMORY_RATIO.get(service) * multiplier
        if service in MINIMUM_MEMORY_MB and allocated_memory < MINIMUM_MEMORY_MB.get(service):
            allocated_memory = MINIMUM_MEMORY_MB.get(service)

        service_memory_config[service], allocated_memory = build_memory_config(service, allocated_memory)

    print_if_verbose('\nMemory distribution for services:')
    for key, value in service_memory_config.items():
        print_if_verbose('{0}, {1}'.format(key, value))
    print_if_verbose('\n')

    return service_memory_config


def append_command(commands, command):
    commands.append('--command')
    commands.append(command)


def build_supervise_script_arguments(service_list, service_memory_config, config, zk):
    commands = []
    commands.append('supervise')

    append_command(commands, ":verify bin/verify-java")
    append_command(commands, ":verify bin/verify-default-ports")
    append_command(commands, ":notify bin/greet")
    append_command(commands, ":kill-timeout 10")

    if zk:
        append_command(commands, "!p10 zk bin/run-zk conf")

    for service in service_list:
        memory_config = service_memory_config.get(service)

        prefix = ''
        if service == MIDDLE_MANAGER:
            prefix = '!p90 '

        if memory_config is None:
            append_command(commands, '{0}{1} bin/run-druid {1} {2}'.format(prefix, service, config))
        else:
            if service == MIDDLE_MANAGER:
                task_config = service_memory_config.get(TASKS)
                task_count = task_config[0]
                task_memory = task_config[1]
                append_command(
                    commands,
                    '{0}{1} bin/run-druid {1} {2} \'{3}\' \'{4} {5}\''
                    .format(prefix, service, config, memory_config, task_count, task_memory))
            elif service == INDEXER:
                task_count = memory_config[0]
                jvm_args = memory_config[1]
                append_command(
                    commands,
                    '{0}{1} bin/run-druid {1} {2} \'{3}\' \'{4}\''
                    .format(prefix, service, config, jvm_args, task_count))
            else:
                append_command(commands,
                               '{0}{1} bin/run-druid {1} {2} \'{3}\''.format(prefix, service, config, memory_config))

    print_if_verbose('Supervise script args:')
    for item in commands:
        print_if_verbose(item)

    print_if_verbose('\n')

    return commands


def main():
    parser = configure_parser()
    args = parser.parse_args()

    global LOGGING_ENABLED
    LOGGING_ENABLED = args.verbose or args.compute

    config, total_memory, service_list, zk, compute = parse_arguments(args)

    # change directory to bin
    os.chdir(os.path.dirname(sys.argv[0]))

    if config == "":
        config = resolve_path('{0}/../{1}'.format(os.getcwd(), BASE_CONFIG_PATH))

    validate_common_directory(config)

    print_startup_config(service_list, config, zk)

    service_memory_config = {}

    if should_compute_memory(config, total_memory, service_list):
        # if memory is to be computed, _common directory should contain common.jvm.config
        validate_common_jvm_args(config)
        memory_in_mega_bytes = convert_total_memory_string(total_memory)
        print_if_verbose('Total memory is {0}m\n'.format(memory_in_mega_bytes))
        memory_to_be_used = check_memory_constraint(memory_in_mega_bytes, service_list)
        print_if_verbose('Memory used for services & tasks {0}m\n'.format(memory_to_be_used))
        service_memory_config = distribute_memory(service_list, memory_to_be_used)
    else:
        print_if_verbose('Not computing memory distribution, reading memory specification from service jvm.config & '
                         'middleManager/runtime.properties\n')

    script_arguments = build_supervise_script_arguments(service_list, service_memory_config, config, zk)

    if compute:
        return

    os.execv('./supervise', script_arguments)


try:
    main()
except (KeyboardInterrupt, ValueError) as error:
    print(error)
    sys.exit(1)
