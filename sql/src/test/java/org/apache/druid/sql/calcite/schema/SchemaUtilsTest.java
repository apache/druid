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

package org.apache.druid.sql.calcite.schema;

import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class SchemaUtilsTest extends CalciteTestBase
{
  private static final String ALLOWED = "allowed";
  private static final AuthenticationResult AUTH_RESULT =
      new AuthenticationResult("someone", "authorizer", null, null);

  private static final AuthorizerMapper AUTHORIZER_MAPPER = new AuthorizerMapper(null)
  {
    @Override
    public Authorizer getAuthorizer(String name)
    {
      return (authenticationResult, resource, action) ->
          ALLOWED.equals(resource.getName()) ? Access.OK : Access.DENIED;
    }
  };

  @Test
  public void testFilterVisibleResourcesMapsManyNamesToOneResource()
  {
    final Resource resource = new Resource(ALLOWED, ResourceType.DATASOURCE);
    Assertions.assertEquals(
        Set.of("a", "b", "c"),
        SchemaUtils.filterVisibleResources(
            AUTHORIZER_MAPPER,
            AUTH_RESULT,
            List.of("a", "b", "c"),
            _ -> resource
        )
    );
  }

  @Test
  public void testFilterVisibleResourcesHidesNamesBehindADeniedResource()
  {
    final Resource resource = new Resource("denied", ResourceType.DATASOURCE);
    Assertions.assertEquals(
        Set.of(),
        SchemaUtils.filterVisibleResources(
            AUTHORIZER_MAPPER,
            AUTH_RESULT,
            List.of("a", "b"),
            _ -> resource
        )
    );
  }

  @Test
  public void testFilterVisibleResourcesAlwaysShowsNamesWithNoResource()
  {
    Assertions.assertEquals(
        Set.of("a", "b"),
        SchemaUtils.filterVisibleResources(AUTHORIZER_MAPPER, AUTH_RESULT, List.of("a", "b"), _ -> null)
    );
  }

  @Test
  public void testFilterVisibleTablesAuthorizesOnEachOwnName()
  {
    Assertions.assertEquals(
        Set.of(ALLOWED),
        SchemaUtils.filterVisibleTables(
            AUTHORIZER_MAPPER,
            AUTH_RESULT,
            List.of(ALLOWED, "other"),
            ResourceType.DATASOURCE
        )
    );
  }

  @Test
  public void testIsTableVisible()
  {
    Assertions.assertTrue(
        SchemaUtils.isTableVisible(AUTHORIZER_MAPPER, AUTH_RESULT, ALLOWED, ResourceType.DATASOURCE)
    );
    Assertions.assertFalse(
        SchemaUtils.isTableVisible(AUTHORIZER_MAPPER, AUTH_RESULT, "other", ResourceType.DATASOURCE)
    );
  }
}
