/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.testing.embedded.auth;

import org.apache.druid.testing.embedded.EmbeddedResource;

/**
 * Runs the same LDAP auth tests as {@link BasicAuthLdapConfigurationTest} but with
 * the reverse group lookup feature enabled ({@code groupBaseDn} and {@code groupSearch}).
 *
 * OpenLDAP (used in the test container) does not return {@code memberOf} in user search
 * results by default. Without reverse group lookup, group-based authorization would fail
 * because the {@code LDAPRoleProvider} cannot resolve group memberships. This test verifies
 * that enabling reverse group lookup allows all group-based authorization to work correctly.
 */
public class BasicAuthLdapReverseGroupLookupTest extends BasicAuthLdapConfigurationTest
{
  @Override
  protected EmbeddedResource getAuthResource()
  {
    return new LdapReverseGroupLookupAuthResource();
  }
}
