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

BASIC_AUTH_BASE = '/druid-ext/basic-security'

AUTHENTICATION_BASE = BASIC_AUTH_BASE + '/authentication'
REQ_AUTHENTICATION_LOAD_STATUS = AUTHENTICATION_BASE + '/loadStatus'
REQ_AUTHENTICATION_REFRESH_ALL = AUTHENTICATION_BASE + '/refreshAll'
AUTHENTICATOR_BASE = AUTHENTICATION_BASE + '/db/{}'
REQ_AUTHENTICATION_USERS = AUTHENTICATOR_BASE + '/users'
REQ_AUTHENTICATION_USER = REQ_AUTHENTICATION_USERS + '/{}'
REQ_AUTHENTICATION_CREDENTIALS = REQ_AUTHENTICATION_USER + '/credentials'

AUTHORIZATION_BASE = BASIC_AUTH_BASE + '/authorization'
REQ_AUTHORIZATION_LOAD_STATUS = AUTHORIZATION_BASE + '/loadStatus'
REQ_AUTHORIZATION_REFRESH_ALL = AUTHORIZATION_BASE + '/refreshAll'
AUTHORIZATION_BASE = AUTHORIZATION_BASE + '/db/{}'
REQ_AUTHORIZATION_USERS = AUTHORIZATION_BASE + '/users'
REQ_AUTHORIZATION_USER = REQ_AUTHORIZATION_USERS + '/{}'
REQ_AUTHORIZATION_USER_ROLES = REQ_AUTHORIZATION_USER + '/roles'
REQ_AUTHORIZATION_USER_ROLE = REQ_AUTHORIZATION_USER_ROLES + '/{}'
REQ_AUTHORIZATION_GROUP_MAPPINGS = AUTHORIZATION_BASE + '/groupMappings'
REQ_AUTHORIZATION_GROUP_MAPPING = AUTHORIZATION_BASE + '/groupMappings/{}'
REQ_AUTHORIZATION_GROUP_ROLES = REQ_AUTHORIZATION_GROUP_MAPPING + '/roles'
REQ_AUTHORIZATION_GROUP_ROLE = REQ_AUTHORIZATION_GROUP_ROLES + '/{}'
REQ_AUTHORIZATION_ROLES = AUTHORIZATION_BASE + '/roles'
REQ_AUTHORIZATION_ROLE = REQ_AUTHORIZATION_ROLES + '/{}'
REQ_AUTHORIZATION_ROLE_PERMISSIONS = REQ_AUTHORIZATION_ROLE + '/permissions'
REQ_USER_MAP = AUTHORIZATION_BASE + '/cachedSerializedUserMap'

class BasicAuthClient:
    '''
    Manage basic security. The Druid session must be logged in with the super
    user, or some other user who has permission to modify user credentials.

    Each client works with one authorizer/authenticator pair. Create multiple clients if you have to
    work with multiple authenticators on a single server.

    The basic pattern to add users and permissions is:

    ```
    # Create a client for your coordinator (Basic auth is not proxied through the router)
    coord = druidapi.jupyter_client('http://localhost:8081', auth=('admin', 'password'))

    # Get a client for your authenticator and authorizer:
    ac = coord.basic_security('yourAuthorizer', 'yourAuthenticator')

    # Create a user in both the authenticator and authorizer
    ac.add_user('bob', 'secret')

    # Define a role
    ac.add_role('myRole')

    # Assign the role to the user
    ac.assign_role_to_user('myRole', 'bob')

    # Give the role some permissions
    ac.grant_permissions('myRole', [[consts.DATASOURCE_RESOURCE, 'foo', consts.READ_ACTION]])
    ```

    Then use the various other methods to list users, roles, and permissions to verify the
    setup. You can then create a second Druid client that acts as the new user:

    ```
    bob_client = druidapi.jupyter_client('http://localhost:8888', auth=('bob', 'secret'))
    ```

    See https://druid.apache.org/docs/latest/operations/security-overview.html#enable-authorizers
    '''

    def __init__(self, rest_client, authenticator, authorizer=None):
        self.rest_client = rest_client
        self.authenticator = authenticator
        self.authorizer = authorizer if authorizer else authenticator

    # Authentication

    def authentication_status(self) -> dict:
        return self.rest_client.get_json(REQ_AUTHENTICATION_LOAD_STATUS)

    def authentication_refresh(self) -> None:
        self.rest_client.get(REQ_AUTHENTICATION_REFRESH_ALL)

    def create_authentication_user(self, user) -> None:
        self.rest_client.post(REQ_AUTHENTICATION_USER, None, args=[self.authenticator, user])

    def set_password(self, user, password) -> None:
        self.rest_client.post_only_json(REQ_AUTHENTICATION_CREDENTIALS, {'password': password}, args=[self.authenticator, user])

    def drop_authentication_user(self, user) -> None:
        self.rest_client.delete(REQ_AUTHENTICATION_USER, args=[self.authenticator, user])
    
    def authentication_user(self, user) -> dict:
        return self.rest_client.get_json(REQ_AUTHENTICATION_USER, args=[self.authenticator, user])
    
    def authentication_users(self) -> list:
        return self.rest_client.get_json(REQ_AUTHENTICATION_USERS, args=[self.authenticator])
    
    # Authorization
    # Groups are not documented. Use at your own risk.

    def authorization_status(self) -> dict:
        return self.rest_client.get_json(REQ_AUTHORIZATION_LOAD_STATUS)

    def authorization_refresh(self) -> None:
        self.rest_client.get(REQ_AUTHORIZATION_REFRESH_ALL)

    def create_authorization_user(self, user) -> None:
        self.rest_client.post(REQ_AUTHORIZATION_USER, None, args=[self.authorizer, user])

    def drop_authorization_user(self, user) -> None:
        self.rest_client.delete(REQ_AUTHORIZATION_USER, args=[self.authenticator, user])
    
    def authorization_user(self, user) -> dict:
        return self.rest_client.get_json(REQ_AUTHORIZATION_USER, args=[self.authorizer, user])
    
    def authorization_users(self) -> list:
        return self.rest_client.get_json(REQ_AUTHORIZATION_USERS, args=[self.authorizer])
    
    def create_group(self, group, payload):
        self.rest_client.post_json(REQ_AUTHORIZATION_GROUP_MAPPING, payload, args=[self.authorizer, group])

    def drop_group(self, group):
        self.rest_client.delete(REQ_AUTHORIZATION_GROUP_MAPPING, args=[self.authorizer, group])

    def groups(self) -> dict:
        return self.rest_client.get_json(REQ_AUTHORIZATION_GROUP_MAPPINGS, args=[self.authorizer])

    def group(self, group) -> dict:
        return self.rest_client.get_json(REQ_AUTHORIZATION_GROUP_MAPPING, args=[self.authorizer, group])
    
    def roles(self):
        return self.rest_client.get_json(REQ_AUTHORIZATION_ROLES, args=[self.authenticator])
 
    def add_role(self, role):
        self.rest_client.post(REQ_AUTHORIZATION_ROLE, None, args=[self.authenticator, role])
   
    def drop_role(self, role):
        self.rest_client.delete(REQ_AUTHORIZATION_ROLE, args=[self.authorizer, role])

    def set_role_permissions(self, role, permissions):
        self.rest_client.post_only_json(REQ_AUTHORIZATION_ROLE_PERMISSIONS, permissions, args=[self.authenticator, role])

    def role_permissions(self, role):
        return self.rest_client.get_json(REQ_AUTHORIZATION_ROLE_PERMISSIONS, args=[self.authenticator, role])
    
    def assign_role_to_user(self, role, user):
        self.rest_client.post(REQ_AUTHORIZATION_USER_ROLE, None, args=[self.authenticator, user, role])

    def revoke_role_from_user(self, role, user):
        self.rest_client.delete(REQ_AUTHORIZATION_USER_ROLE, args=[self.authenticator, user, role])

    def assign_role_to_group(self, group, role):
        self.rest_client.post(REQ_AUTHORIZATION_GROUP_ROLE, None, args=[self.authenticator, group, role])

    def revoke_role_from_group(self, group, role):
        self.rest_client.delete(REQ_AUTHORIZATION_GROUP_ROLE, args=[self.authenticator, group, role])

    def user_map(self):
        # Result uses Smile encoding, not JSON. This is really just for sanity
        # checks: a Python client can't make use of the info.
        # To decode, see newsmile: https://pypi.org/project/newsmile/
        # However, the format Druid returns is not quite compatible with newsmile
        return self.rest_client.get(REQ_USER_MAP, args=[self.authenticator])

    # Convenience methods

    def add_user(self, user, password):
        '''
        Adds a user to both the authenticator and authorizer.
        '''
        self.create_authentication_user(user)
        self.set_password(user, password)
        self.create_authorization_user(user)

    def drop_user(self, user):
        '''
        Drops a user from both the authenticator and authorizer.
        '''
        self.drop_authorization_user(user)
        self.drop_authentication_user(user)

    def users(self):
        '''
        Returns the list of authenticator and authorizer users.
        '''
        return {
            "authenticator": self.authentication_users(),
            "authorizer": self.authorization_users()
        }

    def status(self):
        '''
        Returns both the authenticator and authorizer status.
        '''
        return {
            "authenticator": self.authentication_status(),
            "authorizer": self.authorization_status()
        }
    
    def resource(self, type, name):
        return {
            'type': type,
            'name': name
        }
    
    def action(self, resource, action):
        return {
            'resource': resource,
            'action': action
        }

    def resource_action(self, type, name, action):
        return self.action(self.resource(type, name), action)

    def grant_permissions(self, role, triples):
        '''
        Set the permissions for a role given an array of triples of the form
        `[[type, name, action], ...]`.

        Overwrites any existing permissions.
        '''
        perms = []
        for triple in triples:
            perms.append(self.resource_action(triple[0], triple[1], triple[2]))
        self.set_role_permissions(role, perms)
