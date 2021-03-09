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

package org.apache.druid.sql.calcite;

import org.apache.druid.com.google.common.collect.ImmutableSet;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class DruidPlannerResourceAnalyzeTest extends BaseCalciteQueryTest
{
  @Test
  public void testTable()
  {
    final String sql = "SELECT COUNT(*) FROM foo WHERE foo.dim1 <> 'z'";

    Set<Resource> requiredResources = analyzeResources(
        PLANNER_CONFIG_DEFAULT,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    Assert.assertEquals(
        ImmutableSet.of(
            new Resource("foo", ResourceType.DATASOURCE)
        ),
        requiredResources
    );
  }

  @Test
  public void testConfusingTable()
  {
    final String sql = "SELECT COUNT(*) FROM foo as druid WHERE druid.dim1 <> 'z'";

    Set<Resource> requiredResources = analyzeResources(
        PLANNER_CONFIG_DEFAULT,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    Assert.assertEquals(
        ImmutableSet.of(
            new Resource("foo", ResourceType.DATASOURCE)
        ),
        requiredResources
    );
  }

  @Test
  public void testSubquery()
  {
    final String sql = "SELECT COUNT(*)\n"
                       + "FROM (\n"
                       + "  SELECT DISTINCT dim2\n"
                       + "  FROM druid.foo\n"
                       + "  WHERE SUBSTRING(dim2, 1, 1) IN (\n"
                       + "    SELECT SUBSTRING(dim1, 1, 1) FROM druid.numfoo WHERE dim1 IS NOT NULL\n"
                       + "  )\n"
                       + ")";

    Set<Resource> requiredResources = analyzeResources(
        PLANNER_CONFIG_DEFAULT,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    Assert.assertEquals(
        ImmutableSet.of(
            new Resource("foo", ResourceType.DATASOURCE),
            new Resource("numfoo", ResourceType.DATASOURCE)
        ),
        requiredResources
    );
  }

  @Test
  public void testSubqueryUnion()
  {
    final String sql = "SELECT\n"
                       + "  SUM(cnt),\n"
                       + "  COUNT(*)\n"
                       + "FROM (\n"
                       + "  SELECT dim2, SUM(cnt) AS cnt\n"
                       + "  FROM (SELECT * FROM druid.foo UNION ALL SELECT * FROM druid.foo2)\n"
                       + "  GROUP BY dim2\n"
                       + ")";
    Set<Resource> requiredResources = analyzeResources(
        PLANNER_CONFIG_DEFAULT,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    Assert.assertEquals(
        ImmutableSet.of(
            new Resource("foo", ResourceType.DATASOURCE),
            new Resource("foo2", ResourceType.DATASOURCE)
        ),
        requiredResources
    );
  }

  @Test
  public void testJoin()
  {
    final String sql = "SELECT COUNT(*) FROM foo INNER JOIN numfoo ON foo.dim1 = numfoo.dim1 WHERE numfoo.dim1 <> 'z'";

    Set<Resource> requiredResources = analyzeResources(
        PLANNER_CONFIG_DEFAULT,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    Assert.assertEquals(
        ImmutableSet.of(
            new Resource("foo", ResourceType.DATASOURCE),
            new Resource("numfoo", ResourceType.DATASOURCE)
        ),
        requiredResources
    );
  }

  @Test
  public void testView()
  {
    final String sql = "SELECT COUNT(*) FROM view.aview as druid WHERE dim1_firstchar <> 'z'";

    Set<Resource> requiredResources = analyzeResources(
        PLANNER_CONFIG_DEFAULT,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    Assert.assertEquals(
        ImmutableSet.of(
            new Resource("aview", ResourceType.VIEW)
        ),
        requiredResources
    );
  }

  @Test
  public void testSubqueryView()
  {
    final String sql = "SELECT COUNT(*)\n"
                       + "FROM (\n"
                       + "  SELECT DISTINCT dim2\n"
                       + "  FROM druid.foo\n"
                       + "  WHERE SUBSTRING(dim2, 1, 1) IN (\n"
                       + "    SELECT SUBSTRING(dim1, 1, 1) FROM view.cview WHERE dim2 IS NOT NULL\n"
                       + "  )\n"
                       + ")";

    Set<Resource> requiredResources = analyzeResources(
        PLANNER_CONFIG_DEFAULT,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    Assert.assertEquals(
        ImmutableSet.of(
            new Resource("foo", ResourceType.DATASOURCE),
            new Resource("cview", ResourceType.VIEW)
        ),
        requiredResources
    );
  }

  @Test
  public void testJoinView()
  {
    final String sql = "SELECT COUNT(*) FROM view.cview as aview INNER JOIN numfoo ON aview.dim2 = numfoo.dim2 WHERE numfoo.dim1 <> 'z'";

    Set<Resource> requiredResources = analyzeResources(
        PLANNER_CONFIG_DEFAULT,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    Assert.assertEquals(
        ImmutableSet.of(
            new Resource("cview", ResourceType.VIEW),
            new Resource("numfoo", ResourceType.DATASOURCE)
        ),
        requiredResources
    );
  }

  @Test
  public void testConfusingViewIdentifiers()
  {
    final String sql = "SELECT COUNT(*) FROM view.dview as druid WHERE druid.numfoo <> 'z'";

    Set<Resource> requiredResources = analyzeResources(
        PLANNER_CONFIG_DEFAULT,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    Assert.assertEquals(
        ImmutableSet.of(
            new Resource("dview", ResourceType.VIEW)
        ),
        requiredResources
    );
  }

  @Test
  public void testDynamicParameters()
  {
    final String sql = "SELECT SUBSTRING(dim2, CAST(? as BIGINT), CAST(? as BIGINT)) FROM druid.foo LIMIT ?";
    Set<Resource> requiredResources = analyzeResources(
        PLANNER_CONFIG_DEFAULT,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    Assert.assertEquals(
        ImmutableSet.of(
            new Resource("foo", ResourceType.DATASOURCE)
        ),
        requiredResources
    );
  }
}
