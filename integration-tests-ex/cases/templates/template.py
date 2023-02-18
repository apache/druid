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

'''
Generates a docker-compose.yaml file from a test-specific template. Each
test template either uses the base template directly, or extends the template
to customize bits of the cluster.

Since the cluster is defined as YAML, the cluster definition is build up
internally as a Python data structure made up of maps, arrays and scalars.
PyYaml does the grunt work of converting the data structure to the YAML file.
'''

import yaml, os, os.path
from pathlib import Path

# Constants used frequently in the template.

DRUID_NETWORK = 'druid-it-net'
DRUID_SUBNET = '172.172.172'
ZOO_KEEPER = 'zookeeper'
METADATA = 'metadata'
COORDINATOR = 'coordinator'
OVERLORD = 'overlord'
ROUTER = 'router'
BROKER = 'broker'
HISTORICAL = 'historical'
INDEXER = 'indexer'
MIDDLE_MANAGER = 'middlemanager'

def generate(template_path, template):
    '''
    Main routine to generate a docker-compose file from a script with the
    given template_path, using the template class given. The template path is
    a convenient way to locate directories in the file system using information
    that Python itself provides.
    '''

    # Compute the cluster (test category) name from the template path which
    # we assume to be module/<something>/<template>/<something>.py
    template_path = Path(template_path)
    cluster = template_path.stem

    # Move up to the module (that is, the cases folder) relative to the template file.
    module_dir = Path(__file__).parent.parent

    # The target location for the output file is <module>/target/cluster/<cluster>/docker-compose.yaml
    target_dir = module_dir.joinpath("target")
    target_file = target_dir.joinpath('cluster', cluster, 'docker-compose.yaml')

    # Defer back to the template class to create the output into the docker-compose.yaml file.
    with target_file.open("w") as f:
        template.generate_file(f, cluster)
        f.close()

class BaseTemplate:

    def __init__(self):
        # Cluster is the object tree for the docker-compose.yaml file for our test cluster.
        # The tree is a map of objects, each of which is a map of values. The values are
        # typicaly scalars, maps or arrays. These are represented generically in Python.
        self.cluster = {}

    def generate_file(self, out_file, cluster):
        '''
        Generates the docker-compose.yaml file contents as a header plus a YAML-serialized
        form of the cluster object tree. The file is meant to be generated, used and discarded.
        As a result, we don't worry about generating line-by-line comments: those should appear
        in the template.
        '''
        self.cluster_name = cluster
        self.define_cluster()
        self.out_file = out_file
        self.generate()

    def define_cluster(self):
        '''
        Overall method to define the test cluster.
        '''
        self.define_network()
        self.define_support_services()
        self.define_druid_services()
        self.define_custom_services()

    def define_support_services(self):
        '''
        Define support services which run as containers, but are not provided by
        Druid.
        '''
        self.define_zk()
        self.define_metadata()

    def define_druid_services(self):
        '''
        Define the set of Druid services. Override this method to provide ad-hoc
        services unique to a test. If the test creates multiple versions of a
        service, provide that by overriding the individual service method.
        '''
        self.define_coordinator()
        self.define_overlord()
        self.define_broker()
        self.define_router()
        self.define_historical()
        self.define_indexer()

    def define_custom_services(self):
        '''
        Override to define additional services for the cluster.
        '''
        pass

    def generate(self):
        '''
        Emit output to the target file.
        '''
        self.gen_header()
        self.gen_header_comment()
        self.gen_body()

    def emit(self, text):
        '''
        Emits text to the target file. Used for header comments.
        '''
        # Chop off the newline that occurs when ''' is on a separate line
        if len(text) > 0 and text[0] == '\n':
            text = text[1:]
        self.out_file.write(text)

    def gen_header(self):
        '''
        Emit the standard file header.
        '''
        self.emit('''
# THIS FILE IS GENERATED -- DO NOT EDIT!
#
# Instead, edit the template from which this file was generated.
# Template: templates/{}.py

'''.format(self.cluster_name))

    def gen_header_comment(self):
        '''
        Override to generate a custom header comment after the standard header.
        '''
        pass

    def gen_body(self):
        '''
        Convert the cluster tree into YAML using the pyaml library.
        '''
        try:
            # Version 5.1 or later: sort the keys in the order we created them.
            # This makes doing diffs easier when making changes.
            yaml.dump(self.cluster, self.out_file, sort_keys=False)
        except TypeError:
            # For builds that use pyyaml older than 5.1. Keys will be emitted
            # in random order.
            yaml.dump(self.cluster, self.out_file)

    def define_network(self):
        self.cluster['networks'] = {
            'druid-it-net': {
                'name': DRUID_NETWORK,
                'ipam': {
                    'config': [
                        {'subnet': DRUID_SUBNET + '.0/24'}
                    ]
                }
            }
        }

    def add_service(self, name, service):
        '''
        Add a service to the 'services' key in the cluster tree.
        '''
        services = self.cluster.setdefault('services', {})
        services[name] = service

    def add_volume(self, service, local, container):
        '''
        Adds a volume to a service.
        '''
        volumes = service.setdefault('volumes', [])
        volumes.append(local + ':' + container)

    def add_env(self, service, var, value):
        '''
        Adds an environment variable to a service.
        '''
        vars = service.setdefault('environment', [])
        vars.append(var + '=' + value)

    def add_property(self, service, prop, value):
        '''
        Sets a property for a service. The property is of the same form as the
        .properties file: druid.some.property.
        This method converts the property to the env var form so you don't have to.
        '''
        var = prop.replace('.', '_')
        self.add_env(service, var, value)

    def add_env_file(self, service, env_file):
        '''
        Add an environment file to a service.
        '''
        env_files = service.setdefault('env_file', [])
        env_files.append(env_file)

    def add_env_config(self, service, base_name):
        '''
        Add to a service one of the standard environment config files in
        the Common/environment-configs directory
        '''
        self.add_env_file(service, '../Common/environment-configs/' + base_name + '.env')

    def add_port(self, service, local, container):
        '''
        Add a port mapping to the service
        '''
        ports = service.setdefault('ports', [])
        ports.append(local + ':' + container)

    def define_external_service(self, name) -> dict:
        '''
        Defines a support service external to Druid as a reference to a service
        defined in dependencies.yaml.
        '''
        service = {'extends': {
            'file': '../Common/dependencies.yaml',
            'service': name
            }}
        self.add_service(name, service)
        return service

    def define_zk(self) -> dict:
        '''
        Define the ZooKeeper service. Returns the service.
        '''
        return self.define_external_service(ZOO_KEEPER)

    def define_metadata(self) -> dict:
        '''
        Defines the metadata (MySQL) service. Returns the service
        '''
        return self.define_external_service(METADATA)

    def define_druid_service(self, name, base) -> dict:
        '''
        Defines a Druid service as a reference to the base definition in
        the druid.yaml file. Used when referencing, and extending, a standard
        service definition. Cannot be used for a second instance of a Druid
        service: such services have to be defined from scratch since they
        need unique port mappings and container names.
        '''
        service = {}
        if base is not None:
            service['extends'] = {
                'file': '../Common/druid.yaml',
                'service': base
                }
        self.extend_druid_service(service)
        self.add_service(name, service)
        return service

    def extend_druid_service(self, service):
        '''
        Override this to add options to all Druid services.
        '''
        pass

    def add_depends(self, service, items):
        '''
        Adds a service dependency to a service.
        '''
        if items is not None and len(items) > 0:
            depends = service.setdefault('depends_on', [])
            depends += items

    def define_master_service(self, name, base) -> dict:
        '''
        Defines a "master" service: one which depends on the metadata service.
        '''
        service = self.define_druid_service(name, base)
        self.add_depends(service, [ZOO_KEEPER, METADATA])
        return service

    def define_std_master_service(self, name) -> dict:
        '''
        Defines a "standard" master service in which the service name is
        the same as the service defined in druid.yaml.
        '''
        return self.define_master_service(name, name)

    def define_coordinator(self) -> dict:
        '''
        Defines a coordinator based on the standard definition. Override to
        customize environment variables, mounts, etc.
        '''
        return self.define_std_master_service(COORDINATOR)

    def define_overlord(self) -> dict:
        '''
        Defines an overlord based on the standard definition. Override to
        customize environment variables, mounts, etc.
        '''
        return self.define_std_master_service(OVERLORD)

    def define_worker_service(self, name, base) -> dict:
        '''
        Defines a Druid "worker" service: one that depends only on ZooKeeper.
        '''
        service = self.define_druid_service(name, base)
        self.add_depends(service, [ZOO_KEEPER])
        return service

    def define_std_worker_service(self, name) -> dict:
        '''
        Define a worker service in which the service name for this cluster is the
        same as the service name in druid.yaml.
        '''
        return self.define_worker_service(name, name)

    def define_broker(self) -> dict:
        '''
        Defines a broker based on the standard definition. Override to
        customize environment variables, mounts, etc.
        '''
        return self.define_std_worker_service(BROKER)

    def define_router(self) -> dict:
        '''
        Defines a router based on the standard definition. Override to
        customize environment variables, mounts, etc.
        '''
        return self.define_std_worker_service(ROUTER)

    def define_historical(self) -> dict:
        '''
        Defines a historical based on the standard definition. Override to
        customize environment variables, mounts, etc.
        '''
        return self.define_std_worker_service(HISTORICAL)

    def define_std_indexer(self, base) -> dict:
        '''
        Defines a standard indexer service in which the service name in this
        cluster is the same as the definition in druid.yaml. The service mounts
        the standard data directory.
        '''
        service = self.define_worker_service(INDEXER, base)
        self.define_data_dir(service)
        return service

    def define_data_dir(self, service):
        '''
        Define the input data directory mounted into the selected indexer service.
        '''
        self.add_volume(service, '${MODULE_DIR}/resources', '/resources')

    def define_indexer_service(self) -> dict:
        '''
        Defines an indexer based on the standard definition. Override to
        customize environment variables, mounts, etc.
        '''
        return self.define_std_indexer(INDEXER)

    def define_middle_manager_service(self) -> dict:
        '''
        Defines a middle manager based on the standard definition. Override to
        customize environment variables, mounts, etc.
        '''
        return self.define_std_indexer(MIDDLE_MANAGER)

    def get_indexer_option(self) -> str:
        '''
        Choose which "indexer" to use: middle manager or indexer (the specific service)
        based on the USE_INDEXER environment variable. Defaults to middle manager.
        '''
        value = os.environ.get('USE_INDEXER')
        if value is None:
            value = MIDDLE_MANAGER
        return value

    def define_indexer(self):
        '''
        Defines the cluster's indexer (generic term) service as either indexer (specific
        service) or middle manager, depending on the USE_INDEXER environment variable.
        '''
        value = self.get_indexer_option()
        key = value.lower()
        if key == INDEXER:
            return self.define_indexer_service()
        if key == MIDDLE_MANAGER:
            return self.define_middle_manager_service()
        raise Exception("Invalid USE_INDEXER value: [" + value + ']')

    def define_full_service(self, name, base, host_node):
        '''
        Create a clone of a service as defined in druid.yaml. Use this when
        creating a second instance of a service, since the second must use
        distinct host IP and ports.
        '''
        service = {
            'image': '${DRUID_IT_IMAGE_NAME}',
            'networks': {
                DRUID_NETWORK : {
                    'ipv4_address': DRUID_SUBNET + '.' + str(host_node)
                    }
                },
            'volumes' : [ '${SHARED_DIR}:/shared' ]
            }
        self.add_env_config(service, 'common')
        if base is not None:
            self.add_env_config(service, base)
        self.add_env_file(service, '${OVERRIDE_ENV}')
        self.add_env(service, 'DRUID_INTEGRATION_TEST_GROUP', '${DRUID_INTEGRATION_TEST_GROUP}')
        self.add_service(name, service)
        return service
