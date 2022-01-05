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

package org.apache.druid.curator.discovery;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.discovery.NodeRoles;
import org.apache.druid.guice.annotations.Global;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that the NodeRoles class allows injecting a set of roles
 * which includes custom roles defined by an extension.
 */
public class NodeRolesTest
{
  @Test
  public void testNodeRoles()
  {
    Set<NodeRole> knownRules = NodeRoles.knownRoles();
    assertEquals(NodeRole.values().length, knownRules.size());

    NodeRole customRole = new NodeRole("custom");
    Injector injector = Guice.createInjector(
        binder -> {
          NodeRoles.addKnownRoles(binder);
          NodeRoles.addRole(binder, customRole);
        });
    Set<NodeRole> roles = injector.getInstance(
        Key.get(new TypeLiteral<Set<NodeRole>>(){}, Global.class));
    assertEquals(NodeRole.values().length + 1, roles.size());
    assertTrue(roles.contains(customRole));
  }
}
