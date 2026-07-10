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

package app.druid

import rego.v1

# By default, deny requests.
default allow := false

# Allow admins to do anything.
allow if {
	user_is_admin
}

# Allow the action if the user is granted permission to perform the action.
allow if {
	# Find grants for the user.
	some grant
	user_is_granted[grant]

	# Check if the grant permits the action.
	input.action == grant.action
	regex.match(sprintf("^%s$", [grant.resource.name]), input.resource.name)
	regex.match(sprintf("^%s$", [grant.resource.type]), input.resource.type)
}

# user_is_admin is true if...
user_is_admin if {
	# "admin" is among the user's roles as per data.user_roles
	"admin" in data.user_roles[input.authenticationResult.identity]
}

# user_is_granted is a set of grants for the user identified in the request.
# The `grant` will be contained if the set `user_is_granted` for every...
user_is_granted contains grant if {
	# `role` assigned an element of the user_roles for this user...
	some role in data.user_roles[input.authenticationResult.identity]

	# `grant` assigned a single grant from the grants list for 'role'...
	some grant in data.role_grants[role]
}
