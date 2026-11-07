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

import org.apache.druid.error.DruidException;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.policy.RestrictAllTablesPolicyEnforcer;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

public class SystemTableDataSourceTest
{
  private final SystemTableDataSource dataSource = new SystemTableDataSource("server_properties");

  /** System tables aren't validated by the policy enforcer, even one that requires a policy on every table. */
  @Test
  public void testWithPoliciesReturnsSelfWithoutPolicy()
  {
    Assertions.assertSame(dataSource, dataSource.withPolicies(Map.of(), new RestrictAllTablesPolicyEnforcer(null)));
    Assertions.assertSame(
        dataSource,
        dataSource.withPolicies(Map.of("sys.server_properties", Optional.empty()), NoopPolicyEnforcer.instance())
    );
  }

  /** A query over a system table resolves policies through its children without failing. */
  @Test
  public void testWithPoliciesInsideQueryDataSource()
  {
    final QueryDataSource queryDataSource = new QueryDataSource(
        Druids.newScanQueryBuilder().dataSource(dataSource).eternityInterval().build()
    );
    Assertions.assertEquals(queryDataSource, queryDataSource.withPolicies(Map.of(), NoopPolicyEnforcer.instance()));
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
