import sys
import os
import psutil
import pathlib
from pathlib import Path

QUICKSTART_BASE_CONFIG_PATH = "conf/druid/single-server/quickstart"
HELP_ARG_IDENTIFIER = "help"
COMPUTE_ONLY_ARG_IDENTIFIER = "computeOnly"
RUN_ZK_IDENTIFIER = "runZk"
ROOT_CONFIG_PATH_IDENTIFIER = "rootConfigPath"
SERVICES_IDENTIFIER = "services"
MEMORY_ARG_IDENTIFIER = "totalMemory"
MEMORY_GIGABYTES_IDENTIFIER = "g"
MEMORY_MEGABYTES_IDENTIFIER = "m"
ARG_SEPARATOR = "="
SERVICE_SEPARATOR = ","

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
    MIDDLE_MANAGER_SERVICE_NAME: 1,
    ROUTER_SERVICE_NAME: 2,
    COORDINATOR_SERVICE_NAME: 36,
    BROKER_SERVICE_NAME: 56,
    HISTORICAL_SERVICE_NAME: 90
}

SERVICE_MEMORY_LOWER_BOUND = {
    MIDDLE_MANAGER_SERVICE_NAME: 64,
    ROUTER_SERVICE_NAME: 128
}

SERVICE_MEMORY_HEAP_PERCENTAGE = {
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
    root_config_path = ""
    total_memory = ""
    compute_only = False
    run_zk = False

    for argument in sys.argv[1:]:
        if check_argument_type(argument, COMPUTE_ONLY_ARG_IDENTIFIER):
            compute_only = True
        elif check_argument_type(argument, RUN_ZK_IDENTIFIER):
            run_zk = True
        elif check_argument_type(argument, ROOT_CONFIG_PATH_IDENTIFIER):
            root_config_path = os.path.join(os.getcwd(), get_argument_value(argument))
            if os.path.exists(root_config_path) is False:
                raise Exception(f'rootConfigPath `{root_config_path}` doesn\'t exist')
        elif (check_argument_type(argument, MEMORY_ARG_IDENTIFIER)):
            total_memory = get_argument_value(argument)
        elif (check_argument_type(argument, SERVICES_IDENTIFIER)):
            split_args = argument.split(ARG_SEPARATOR)
            services_arg = split_args[1]
            services = services_arg.split(SERVICE_SEPARATOR)

            for service in services:
                if service not in DEFAULT_SERVICES:
                    raise Exception(f'{service} is not a valid service name, should be one of {DEFAULT_SERVICES}')

                if service in service_list:
                    raise Exception(f'{service} is specified multiple times')

                service_list.append(service)

    if len(service_list) == 0:
        # start all services
        service_list = DEFAULT_SERVICES
        run_zk = True

    return root_config_path, total_memory, service_list, run_zk, compute_only

def print_startup_config(service_list, root_config_path, run_zk):
    print(f'starting {service_list}, using config from {root_config_path}')
    if run_zk:
        zk_config_path = pathlib.Path(f'{os.getcwd()}/../conf/zk').resolve()
        print(f'starting zk, using default config from {zk_config_path}')
    print('\n')

def should_compute_memory(root_config_path, total_memory, service_list):
    # if jvm file is present for any of the services
    # it should be present for all services and totalMemory should not be specified
    # if totalMemory is given, jvm file shouldn't be present for any service

    jvm_config_count = 0
    for service in service_list:
        if Path(f'{root_config_path}/{service}/jvm.config').is_file():
            jvm_config_count += 1
        elif jvm_config_count > 0:
            raise Exception('jvm.config file is missing for service {service}, jvm.config should be specified for all the services or none')

    if jvm_config_count > 0 and (jvm_config_count != len(service_list) or total_memory != ""):
        if jvm_config_count != len(service_list):
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
        print(f'`{MEMORY_ARG_IDENTIFIER}` argument is not specified, Druid will use 80% of system memory: {computed_memory}m')
        return computed_memory
    elif memory.endswith(MEMORY_MEGABYTES_IDENTIFIER):
        return int(memory[:-1])
    elif memory.endswith(MEMORY_GIGABYTES_IDENTIFIER):
        return 1024 * int(memory[:-1])
    else:
        raise Exception('Incorrect format for totalMemory argument, expected format is <integer_value><m/g>')

def build_memory_config_string(heap_memory, direct_memory):
    if direct_memory == 0:
        return f'-Xms{heap_memory}m -Xmx{heap_memory}m'
    return f'-Xms{heap_memory}m -Xmx{heap_memory}m -XX:MaxDirectMemorySize={direct_memory}m'

def distribute_memory_over_services(service_list, total_memory):
    service_memory_config = {}

    memory_weight_sum = 0
    for service in service_list:
        memory_weight_sum += SERVICE_MEMORY_DISTRIBUTION_WEIGHT.get(service)

    multiplier = total_memory / memory_weight_sum

    lower_bound_memory_allocation = 0
    allocated_services = set()

    for service in service_list:
        allocated_memory = SERVICE_MEMORY_DISTRIBUTION_WEIGHT.get(service) * multiplier
        if service in SERVICE_MEMORY_LOWER_BOUND and allocated_memory < SERVICE_MEMORY_LOWER_BOUND.get(service):
            allocated_memory = SERVICE_MEMORY_LOWER_BOUND.get(service)
            heap_memory = SERVICE_MEMORY_HEAP_PERCENTAGE.get(service) * allocated_memory
            direct_memory = allocated_memory - heap_memory
            service_memory_config[service] = build_memory_config_string(int(heap_memory), int(direct_memory))
            lower_bound_memory_allocation += allocated_memory
            allocated_services.add(service)

    if lower_bound_memory_allocation > 0:
        # compute the multiplier again for remaing services
        memory_weight_sum = 0
        for service in service_list:
            if service in allocated_services:
                continue
            memory_weight_sum += SERVICE_MEMORY_DISTRIBUTION_WEIGHT.get(service)
        multiplier = (total_memory - lower_bound_memory_allocation) / memory_weight_sum

    for service in service_list:
        if service in allocated_services:
            continue
        allocated_memory = SERVICE_MEMORY_DISTRIBUTION_WEIGHT.get(service) * multiplier
        if service in SERVICE_MEMORY_LOWER_BOUND and allocated_memory < SERVICE_MEMORY_LOWER_BOUND.get(service):
            allocated_memory = SERVICE_MEMORY_LOWER_BOUND.get(service)

        heap_memory = SERVICE_MEMORY_HEAP_PERCENTAGE.get(service) * allocated_memory
        direct_memory = allocated_memory - heap_memory
        service_memory_config[service] = build_memory_config_string(int(heap_memory), int(direct_memory))

    print(f'\nMemory distribution for services:')
    for key, value in service_memory_config.items():
        print(f'{key}, memory_config: {value}')
    print('\n')

    return service_memory_config

def build_supervise_script_arguments(service_list, service_memory_config, root_config_path, run_zk):
    argument_list = []

    argument_list.append("\":verify bin/verify-java\"")
    argument_list.append("\":verify bin/verify-default-ports\"")
    argument_list.append("\":notify bin/greet\"")
    argument_list.append("\":kill-timeout 10\"")

    if run_zk:
        argument_list.append("\"!p10 zk bin/run-zk conf\"")

    for service in service_list:
        prefix = ''
        if service == MIDDLE_MANAGER_SERVICE_NAME:
            prefix = '!p90 '

        jvm_args = service_memory_config.get(service)

        if jvm_args is None:
            argument_list.append(f'\"{prefix}{service} bin/run-druid {service} {root_config_path}\"')
        else:
            argument_list.append(f'\"{prefix}{service} bin/run-druid {service} {root_config_path} \'{jvm_args}\'\"')

    print('Supervise script command:')
    for item in argument_list:
        print(item)

    print('\n')

    return ",".join(argument_list)

def display_help():
    text = """
    Usage: start-druid [options]

    where options include:
       totalMemory=<value>
            Integer value is supported with 'm' or 'g' suffix.
            Memory for druid services, if totalMemory is not specified
            80 percent of system memory is used.
            Note, if service specific jvm config is present,
            <totalMemory> shouldn't be specified.
            <totalMemory> should be greater than equals 3g
       rootConfigPath=<value>
            Directory containing common and service specific
            properties to be overridden, this directory must contain '_common'
            directory with 'common.jvm.config' & 'common.runtime.properties'
            files.
            If <rootConfigPath> is not specified, config from
            'conf/druid/single-server/quickstart' directory is used.
            This path is relative to current working directory
       services=<value>
            Value is comma separated string.
            List of services to be started, should be a subset of
            [broker, router, middleManager, historical, coordinator-overlord].
            If runtime or jvm properties are to be overridden, they should be
            kept within <rootConfigPath>/<service>.
            If <rootConfigPath> is not specified config files can also be
            placed within `conf/druid/single-server/quickstart/<service>`
            directory.
            Note, if jvm.config file is present for one of the services,
            it must be present for all services.
            If <services> argument is not given, all services
            alongwith zookeeper is started.
       runZk
            Specification to run zookeeper, zk config is picked up from conf/zk.
       computeOnly
            Validate the arguments and display memory distribution for services.

    sample usage:
        start-druid
            Start up all the services (including zk)
            using 80% of system memory.
        start-druid totalMemory=100g
            Start up all the services (including zk)
            using the given memory.
        start-druid totalMemory=100g computeOnly
            Compute memory distribution and validate
            arguments for starting all the services.
        start-druid totalMemory=100g services=broker,router
            Start `broker` & `router` service, using `100g` of memory.
            Read config from conf/druid/single-server/quickstart.
        start-druid totalMemory=100g rootConfigPath=conf/druid/single-server/custom services=broker,router
            Start `broker` & `router` service, using 100g of memory.
            Read config from <rootConfigPath>.
            Since <totalMemory> is specified, exception is thrown if
            jvm.config is present for any of the services.
        start-druid rootConfigPath=conf/druid/single-server/custom services=broker,router
            Start `broker` & `router` service,
            using 80% of system memory.
            If jvm.config is specified for both the
            services within <rootConfigPath>/<service>,
            memory distribution is not calculated.
            If jvm.config is present for either of the services,
            exception is thrown.
            If jvm.config is not present for both of the services,
            memory distribution is calculated.
        start-druid totalMemory=100g rootConfigPath=conf/druid/single-server/custom services=broker,router runZk
            Start zookeeper alongwith other services.
            zk config is read from conf/zk.
    """

    print(text)


def main():
    for argument in sys.argv[1:]:
        if check_argument_type(argument, HELP_ARG_IDENTIFIER):
            display_help()
            return

    print("Druid quickstart\n")

    root_config_path, total_memory, service_list, run_zk, compute_only = parse_arguments()

    # change directory to bin
    os.chdir(os.path.dirname(sys.argv[0]))

    if root_config_path == "":
        root_config_path = pathlib.Path(f'{os.getcwd()}/../{QUICKSTART_BASE_CONFIG_PATH}').resolve()

    print_startup_config(service_list, root_config_path, run_zk)

    service_memory_config = {}
    if (should_compute_memory(root_config_path, total_memory, service_list)):
        memory_in_mega_bytes = convert_total_memory_string(total_memory)
        service_memory_config = distribute_memory_over_services(service_list, memory_in_mega_bytes)
    else:
        print('not computing memory distribution, reading memory specification from service jvm.config\n')

    script_arguments = build_supervise_script_arguments(service_list, service_memory_config, root_config_path, run_zk)

    if compute_only:
        return

    os.system(f'exec ./supervise -a {script_arguments}')

if __name__ == '__main__':
    main()
