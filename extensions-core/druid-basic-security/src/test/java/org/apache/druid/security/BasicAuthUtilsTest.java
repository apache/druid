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

package org.apache.druid.security;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.BasicSecurityDruidModule;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerPermission;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class BasicAuthUtilsTest
{
  @Test
  public void testPermissionSerdeIsChillAboutUnknownEnumStuffs() throws JsonProcessingException
  {
    final String someRoleName = "some-role";
    final String otherRoleName = "other-role";
    final String thirdRoleName = "third-role";
    final ResourceAction fooRead = new ResourceAction(new Resource("foo", ResourceType.DATASOURCE), Action.READ);
    final ResourceAction barRead = new ResourceAction(new Resource("bar", ResourceType.DATASOURCE), Action.READ);
    final ResourceAction customRead = new ResourceAction(new Resource("bar", "CUSTOM"), Action.READ);

    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    mapper.registerModules(new BasicSecurityDruidModule().getJacksonModules());
    Map<String, Object> rawMap = new HashMap<>();
    rawMap.put(
        someRoleName,
        new BasicAuthorizerRole(
          someRoleName,
          BasicAuthorizerPermission.makePermissionList(
              ImmutableList.of(
                  fooRead,
                  barRead
              )
          )
      )
    );
    // custom ResourceType
    rawMap.put(
        otherRoleName,
        ImmutableMap.of(
            "name",
            otherRoleName,
            "permissions",
            ImmutableList.of(
                ImmutableMap.of(
                    "resourceAction", fooRead,
                    "resourceNamePattern", "foo"
                ),
                ImmutableMap.of(
                    "resourceAction", customRead,
                    "resourceNamePattern", "bar"
                )
            )
        )
    );
    // bad Action
    rawMap.put(
        thirdRoleName,
        ImmutableMap.of(
            "name",
            thirdRoleName,
            "permissions",
            ImmutableList.of(
                ImmutableMap.of(
                    "resourceAction",
                    ImmutableMap.of(
                        "resource",
                        ImmutableMap.of("name", "some-view", "type", "VIEW"),
                        "action", "READ"
                    ),
                    "resourceNamePattern", "some-view"
                ),
                ImmutableMap.of(
                    "resourceAction",
                    ImmutableMap.of(
                        "resource",
                        ImmutableMap.of("name", "foo", "type", "DATASOURCE"),
                        "action", "UNKNOWN"
                    ),
                    "resourceNamePattern", "some-view"
                )
            )
        )
    );
    byte[] mapBytes = mapper.writeValueAsBytes(rawMap);
    Map<String, BasicAuthorizerRole> roleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(mapper, mapBytes);
    Assert.assertNotNull(roleMap);
    Assert.assertEquals(3, roleMap.size());

    Assert.assertTrue(roleMap.containsKey(someRoleName));
    Assert.assertEquals(2, roleMap.get(someRoleName).getPermissions().size());
    Assert.assertEquals(
        BasicAuthorizerPermission.makePermissionList(ImmutableList.of(fooRead, barRead)),
        roleMap.get(someRoleName).getPermissions()
    );

    // this one has custom resource type... this test is somewhat pointless, it made more sense when type was an enum
    Assert.assertTrue(roleMap.containsKey(otherRoleName));
    Assert.assertEquals(2, roleMap.get(otherRoleName).getPermissions().size());
    Assert.assertEquals(
        BasicAuthorizerPermission.makePermissionList(ImmutableList.of(fooRead, customRead)),
        roleMap.get(otherRoleName).getPermissions()
    );

    // this one has an unknown Action, expect only 1 permission to deserialize correctly and failure ignored
    Assert.assertTrue(roleMap.containsKey(thirdRoleName));
    Assert.assertEquals(1, roleMap.get(thirdRoleName).getPermissions().size());
    Assert.assertEquals(
        BasicAuthorizerPermission.makePermissionList(
            ImmutableList.of(new ResourceAction(new Resource("some-view", ResourceType.VIEW), Action.READ))
        ),
        roleMap.get(thirdRoleName).getPermissions()
    );
  }
}
