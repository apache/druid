import sys
import os
import psutil
from pathlib import Path

SUPERVISE_CONFIG_FILE_PATH = "../conf/supervise/single-server/quickstart.conf"
QUICKSTART_BASE_CONFIG_PATH = "conf/druid/single-server/quickstart"
HELP_ARG_IDENTIFIER = "help"
COMPUTE_ONLY_ARG_IDENTIFIER = "computeOnly"
RUN_ZK_IDENTIFIER = "runZk"
BASE_CONFIG_PATH_IDENTIFIER = "baseConfigPath"
MEMORY_ARG_IDENTIFIER = "totalMemory"
MEMORY_GIGABYTES_IDENTIFIER = "g"
MEMORY_MEGABYTES_IDENTIFIER = "m"
ARG_SEPARATOR = "="

BROKER_SERVICE_NAME = "broker"
ROUTER_SERVICE_NAME = "router"
COORDINATOR_SERVICE_NAME = "coordinator-overlord"
HISTORICAL_SERVICE_NAME = "historical"
MIDDLE_MANAGER_SERVICE_NAME = "middleManager"

DEFAULT_SERVICES = [
    BROKER_SERVICE_NAME,
    ROUTER_SERVICE_NAME,
    COORDINATOR_SERVICE_NAME,
    HISTORICAL_SERVICE_NAME,
    MIDDLE_MANAGER_SERVICE_NAME
]

SERVICE_MEMORY_DISTRIBUTION_WEIGHT = {
    MIDDLE_MANAGER_SERVICE_NAME: 0.25,
    ROUTER_SERVICE_NAME: 1,
    COORDINATOR_SERVICE_NAME: 18,
    BROKER_SERVICE_NAME: 28,
    HISTORICAL_SERVICE_NAME: 50
}

SERVICE_MEMORY_HEAP_RATIO = {
    MIDDLE_MANAGER_SERVICE_NAME: 1,
    ROUTER_SERVICE_NAME: 1,
    COORDINATOR_SERVICE_NAME: 1,
    BROKER_SERVICE_NAME: 0.60,
    HISTORICAL_SERVICE_NAME: 0.40
}

def check_argument_type(argument, type):
    split_args = argument.split(ARG_SEPARATOR)
    return split_args[0] == type

def get_argument_value(argument):
    split_args = argument.split(ARG_SEPARATOR)
    return split_args[1]

def parse_arguments():
    service_list = []
    service_path_list = []
    base_config_path = ""
    total_memory = ""
    compute_only = False
    run_zk = False

    for argument in sys.argv[1:]:
        if check_argument_type(argument, COMPUTE_ONLY_ARG_IDENTIFIER):
            compute_only = True
        elif check_argument_type(argument, RUN_ZK_IDENTIFIER):
            run_zk = True
        elif check_argument_type(argument, BASE_CONFIG_PATH_IDENTIFIER):
            base_config_path = os.path.join(os.getcwd(), get_argument_value(argument))
        elif (check_argument_type(argument, MEMORY_ARG_IDENTIFIER)):
            total_memory = get_argument_value(argument)
        else:
            split_args = argument.split(ARG_SEPARATOR)
            service = split_args[0]

            if service not in DEFAULT_SERVICES:
                raise Exception(f'{service} is not a valid service name, should be one of {DEFAULT_SERVICES}')

            path = ""

            if len(split_args) == 2:
                path = split_args[1]
            complete_path = os.path.join(base_config_path, path)
            if os.path.exists(os.path.join(complete_path)) is False:
                raise Exception(f'Path `{complete_path}` specified for service `{service}` doesn\'t exist ')
            service_list.append(service)
            service_path_list.append(path)

    if len(service_list) == 0:
        # start all services
        service_list = DEFAULT_SERVICES
        service_path_list = [""] * len(DEFAULT_SERVICES)
        run_zk = True

    return base_config_path, total_memory, list(zip(service_list, service_path_list)), run_zk, compute_only

def should_compute_memory(base_config_path, total_memory, service_config):
    # if jvm file is present for any of the services
    # it should be present for all services and totalMemory should not be specified
    # if totalMemory is given, jvm file shouldn't be present for any service

    jvm_config_count = 0
    for item in service_config:
        if item[1] != "":
            if Path(f'{base_config_path}/{item[1]}/jvm.config').is_file():
                jvm_config_count += 1
            elif jvm_config_count > 0:
                raise Exception('jvm.config file is missing for service {item[0]}, jvm.config should be specified for all the services or none')

    if jvm_config_count > 0 and (jvm_config_count != len(service_config) or total_memory != ""):
        if jvm_config_count != len(service_config):
            raise Exception("jvm.config file should be present for all services or none")
        if total_memory != "":
            raise Exception("If jvm.config is given for services, `totalMemory` argument shouldn't be specified")

    return jvm_config_count == 0

def compute_system_memory():
    system_memory = psutil.virtual_memory().total # mem in bytes
    memory_for_druid = int((system_memory * 0.8) / (1024 * 1024))
    return memory_for_druid

def convert_total_memory_string(memory):
    if memory == "":
        computed_memory = compute_system_memory()
        print(f'`{MEMORY_ARG_IDENTIFIER}` argument is not specified Druid will use 80% of system memory: {computed_memory}m')
        return computed_memory
    elif memory.endswith(MEMORY_MEGABYTES_IDENTIFIER):
        return int(memory[:-1])
    elif memory.endswith(MEMORY_GIGABYTES_IDENTIFIER):
        return 1024 * int(memory[:-1])
    else:
        raise Exception('Incorrect format for totalMemory argument, expected format is <value>m or <value>g')

def build_memory_config_string(heap_memory, direct_memory):
    if direct_memory == 0:
        return f'-Xms{heap_memory}m -Xmx{heap_memory}m'
    return f'-Xms{heap_memory}m -Xmx{heap_memory}m -XX:MaxDirectMemorySize={direct_memory}m'

def distribute_memory_over_services(service_config, total_memory):
    service_memory_config = {}
    service_instance_map = {}

    for item in service_config:
        service_instance_map[item[0]] = service_instance_map.get(item[0], 0) + 1

    memory_weight_sum = 0
    for key, value in service_instance_map.items():
        memory_weight_sum += SERVICE_MEMORY_DISTRIBUTION_WEIGHT.get(key) * value

    multiplier = total_memory / memory_weight_sum

    for key, value in service_instance_map.items():
        allocated_memory = SERVICE_MEMORY_DISTRIBUTION_WEIGHT.get(key) * multiplier
        heap_memory = SERVICE_MEMORY_HEAP_RATIO.get(key) * allocated_memory
        direct_memory = allocated_memory - heap_memory
        service_memory_config[key] = build_memory_config_string(int(heap_memory), int(direct_memory))

    print(f'\nMemory distribution for services:')
    for key, value in service_memory_config.items():
        print(f'{key}, memory_config: {value}, instance_count: {service_instance_map[key]}')
    print('\n')

    return service_memory_config

def create_supervise_config_file(service_config, service_memory_config, base_config_path, run_zk):

    with open(SUPERVISE_CONFIG_FILE_PATH, 'w+') as the_file:
        the_file.write(":verify bin/verify-java\n")
        the_file.write(":verify bin/verify-default-ports\n")
        the_file.write(":notify bin/greet\n")
        the_file.write(":kill-timeout 10\n")
        the_file.write("\n")

        if run_zk:
            the_file.write("!p10 zk bin/run-zk conf\n")

        for item in service_config:
            service = item[0]
            prefix = ''
            if service == MIDDLE_MANAGER_SERVICE_NAME:
                prefix = '!p90 '
            if item[1] == "":
               service_path = item[0]
            else:
                service_path = item[1]
            jvm_args = service_memory_config.get(item[0])

            if jvm_args == "":
                the_file.write(f'{prefix}{service} bin/run-druid {service} {base_config_path} {service_path}\n')
            else:
                the_file.write(f'{prefix}{service} bin/run-druid {service} {base_config_path} {service_path} \'{jvm_args}\'\n')

def print_service_config(service_config, base_config_path):
    print('Services to start:')
    for item in service_config:
        if item[1] == "":
            print(f'{item[0]}, using default config from {os.getcwd()}/../{QUICKSTART_BASE_CONFIG_PATH}')
        else:
            print(f'{item[0]}, using config from {base_config_path}/{item[1]}')

def display_help():
    text = """
    Usage: start-druid [options]

    where options include:
       totalMemory=<memory>
            memory for druid cluster, if totalMemory is not specified
            80 percent of system memory is used. Note, if service
            specific jvm config is present,
            totalMemory shouldn't be specified
       baseConfigPath=<path>
            relative path to base directory containing common and service specific
            properties to be overridden, this directory must contain `_common`
            directory with `common.jvm.config` & `common.runtime.properties`
            if `baseConfigPath` is not specified, config from
            conf/druid/single-server/quickstart direcotry is used
       computeOnly
            command dry-run, validates the arguments and
            display the memory distribution for services
       runZk
            specification to run zookeeper, zk config is picked up from conf/zk
       <service_identifier>=[subdirectory]
            service_identifier is the service to be started, multiple services
            can be specified, `service_identifier` should be one of
            [broker, router, middleManager, historical, coordinator-overlord]
            `subdirectory` is optional directory within `baseConfigPath`
            containing runtime properties or/and jvm properties
            Note, if jvm.config file is present for one service, it must be
            present for all other services
            If no service is specified, all services and zookeeper are started

    sample usage:
        start-druid
            start up all the services using the default system memory
        start-druid totalMemory=100g
            start up all the services using the given memory
        start-druid totalMemory=100g computeOnly
            compute memory distribution for all the services
        start-druid totalMemory=100g broker router historical
            starts `broker`, `router` and `historical` services, using `100g` of memory
        start-druid totalMemory=100g baseConfigPath=/conf/druid/single-server/large broker router historical
            starts `broker`, `router` and `historical` service, using 100g of memory,
            use common configs from specified `baseConfigPath`
        start-druid totalMemory=100g baseConfigPath=/conf/druid/single-server/large broker=broker router=router historical=historical
            starts `broker`, `router` and `historical` services, using 100g of memory, use common configs
            from specified `baseConfigPath`, use service specific config from specified directories
            if jvm.config is specified for all the services, memory distribution is not computed
        start-druid totalMemory=100g baseConfigPath=/conf/druid/single-server/large broker=broker1 broker=broker2
            starts 2 instances of `broker`
            config is read from respective directories, depending on whether jvm.config is specified,
            memory distribution is computed
        start-druid totalMemory=100g baseConfigPath=conf/druid/profile broker=broker1 historical=historical1
            if either of `broker1`, `historical1` subdirectory contains jvm.config,
            exception is thrown since `totalMemory` argument is specified
        start-druid baseConfigPath=conf/druid/profile broker=broker1 historical=historical1
            exception is thrown if either of `broker1`, `historical1`
            subdirectory contains jvm.config but not both
            If none of the subdirectory contains jvm.config, memory distribution is computed
    """

    print(text)


def main():
    for argument in sys.argv[1:]:
        if check_argument_type(argument, HELP_ARG_IDENTIFIER):
            display_help()
            return

    print("Druid automated quickstart\n")

    base_config_path, total_memory, service_config, run_zk, compute_only = parse_arguments()

    # change directory to bin
    os.chdir(os.path.dirname(sys.argv[0]))

    print(f'Arguments passed: baseConfigPath: "{base_config_path}", totalMemory: "{total_memory}"\n')
    print_service_config(service_config, base_config_path)

    service_memory_config = {}
    if (should_compute_memory(base_config_path, total_memory, service_config)):
        memory_in_mega_bytes = convert_total_memory_string(total_memory)
        service_memory_config = distribute_memory_over_services(service_config, memory_in_mega_bytes)

    if compute_only:
        return

    if base_config_path == "":
        base_config_path = QUICKSTART_BASE_CONFIG_PATH

    create_supervise_config_file(service_config, service_memory_config, base_config_path, run_zk)

    os.system(f'exec ./supervise -c {SUPERVISE_CONFIG_FILE_PATH}')

if __name__ == '__main__':
    main()