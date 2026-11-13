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


package org.apache.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.policy.RestrictAllTablesPolicyEnforcer;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

public class SystemTableDataSourceTest
{
  private final SystemTableDataSource dataSource = new SystemTableDataSource("server_properties");

  /**
   * System tables aren't validated by the policy enforcer, even one that requires a policy on every table, and
   * attaching policies keeps the attached access, including inside a subquery.
   */
  @Test
  public void testWithPoliciesReturnsSelfWithoutPolicy()
  {
    Assertions.assertSame(dataSource, dataSource.withPolicies(Map.of(), new RestrictAllTablesPolicyEnforcer(null)));
    Assertions.assertSame(
        dataSource,
        dataSource.withPolicies(Map.of("sys.server_properties", Optional.empty()), NoopPolicyEnforcer.instance())
    );

    final SystemTableDataSource withAccess = dataSource.withAccess(SystemTableAccess.ALL_ROWS);
    final QueryDataSource queryDataSource = new QueryDataSource(
        Druids.newScanQueryBuilder().dataSource(withAccess).eternityInterval().build()
    );
    final DataSource withPolicies = queryDataSource.withPolicies(Map.of(), NoopPolicyEnforcer.instance());
    Assertions.assertSame(withAccess, withPolicies.getChildren().get(0));
  }

  /**
   * Neither the access nor the scope that a server attaches is serialized, so a client can't supply them: a table read
   * from JSON has no access and the local scope.
   */
  @Test
  public void testAccessAndScopeAreNotSerialized() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    final SystemTableDataSource attached =
        dataSource.withAccess(SystemTableAccess.ALL_ROWS).withScope(SystemTableDataSource.Scope.CLUSTER);

    Assertions.assertEquals(mapper.writeValueAsString(dataSource), mapper.writeValueAsString(attached));
    final SystemTableDataSource deserialized = (SystemTableDataSource) mapper.readValue(
        "{\"type\":\"systemTable\",\"table\":\"server_properties\","
        + "\"access\":{\"rowPredicate\":null},\"scope\":\"CLUSTER\"}",
        DataSource.class
    );
    Assertions.assertNull(deserialized.getAccess());
    Assertions.assertEquals(SystemTableDataSource.Scope.LOCAL, deserialized.getScope());
    Assertions.assertEquals(dataSource, deserialized);
  }

  /**
   * A system table can be processed only once a server has attached the caller's access.
   */
  @Test
  public void testProcessableOnlyWithAccess()
  {
    Assertions.assertFalse(dataSource.isProcessable());
    Assertions.assertTrue(dataSource.withAccess(SystemTableAccess.ALL_ROWS).isProcessable());
  }

  @Test
  public void testReadsSystemTable()
  {
    Assertions.assertTrue(SystemTableDataSource.readsSystemTable(dataSource));
    Assertions.assertTrue(
        SystemTableDataSource.readsSystemTable(
            new QueryDataSource(Druids.newScanQueryBuilder().dataSource(dataSource).eternityInterval().build())
        )
    );
    Assertions.assertFalse(SystemTableDataSource.readsSystemTable(new TableDataSource("wikipedia")));
  }

  /**
   * Setting the scope of a datasource sets it on every system table it reads, including in subqueries.
   */
  @Test
  public void testWithScopeAppliesToNestedSystemTables()
  {
    final DataSource scoped = SystemTableDataSource.withScope(
        new QueryDataSource(Druids.newScanQueryBuilder().dataSource(dataSource).eternityInterval().build()),
        SystemTableDataSource.Scope.CLUSTER
    );

    Assertions.assertEquals(
        SystemTableDataSource.Scope.CLUSTER,
        ((SystemTableDataSource) scoped.getChildren().get(0)).getScope()
    );
    Assertions.assertSame(
        dataSource.getScope(),
        SystemTableDataSource.Scope.LOCAL
    );
  }

  @Test
  public void testWithPoliciesRejectsPolicy()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> dataSource.withPolicies(
            Map.of("sys.server_properties", Optional.of(RowFilterPolicy.from(new NullFilter("server", null)))),
            NoopPolicyEnforcer.instance()
        )
    );
    Assertions.assertEquals("System table[server_properties] doesn't support policies", e.getMessage());
  }
}
