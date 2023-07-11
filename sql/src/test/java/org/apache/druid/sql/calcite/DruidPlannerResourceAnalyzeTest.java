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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DruidPlannerResourceAnalyzeTest extends BaseCalciteQueryTest
{
  @Test
  public void testTable()
  {
    final String sql = "SELECT COUNT(*) FROM foo WHERE foo.dim1 <> 'z'";

    analyzeResources(
        sql,
        ImmutableList.of(
            new ResourceAction(new Resource("foo", ResourceType.DATASOURCE), Action.READ)
        )
    );
  }

  @Test
  public void testConfusingTable()
  {
    final String sql = "SELECT COUNT(*) FROM foo as druid WHERE druid.dim1 <> 'z'";

    analyzeResources(
        sql,
        ImmutableList.of(
            new ResourceAction(new Resource("foo", ResourceType.DATASOURCE), Action.READ)
        )
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

    analyzeResources(
        sql,
        ImmutableList.of(
            new ResourceAction(new Resource("foo", ResourceType.DATASOURCE), Action.READ),
            new ResourceAction(new Resource("numfoo", ResourceType.DATASOURCE), Action.READ)
        )
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
    analyzeResources(
        sql,
        ImmutableList.of(
            new ResourceAction(new Resource("foo", ResourceType.DATASOURCE), Action.READ),
            new ResourceAction(new Resource("foo2", ResourceType.DATASOURCE), Action.READ)
        )
    );
  }

  @Test
  public void testJoin()
  {
    final String sql = "SELECT COUNT(*) FROM foo INNER JOIN numfoo ON foo.dim1 = numfoo.dim1 WHERE numfoo.dim1 <> 'z'";

    analyzeResources(
        sql,
        ImmutableList.of(
            new ResourceAction(new Resource("foo", ResourceType.DATASOURCE), Action.READ),
            new ResourceAction(new Resource("numfoo", ResourceType.DATASOURCE), Action.READ)
        )
    );
  }

  @Test
  public void testView()
  {
    final String sql = "SELECT COUNT(*) FROM view.aview as druid WHERE dim1_firstchar <> 'z'";

    analyzeResources(
        sql,
        ImmutableList.of(
            new ResourceAction(new Resource("aview", ResourceType.VIEW), Action.READ)
        )
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

    analyzeResources(
        sql,
        ImmutableList.of(
            new ResourceAction(new Resource("foo", ResourceType.DATASOURCE), Action.READ),
            new ResourceAction(new Resource("cview", ResourceType.VIEW), Action.READ)
        )
    );
  }

  @Test
  public void testJoinView()
  {
    final String sql = "SELECT COUNT(*) FROM view.cview as aview INNER JOIN numfoo ON aview.dim2 = numfoo.dim2 WHERE numfoo.dim1 <> 'z'";

    analyzeResources(
        sql,
         ImmutableList.of(
            new ResourceAction(new Resource("cview", ResourceType.VIEW), Action.READ),
            new ResourceAction(new Resource("numfoo", ResourceType.DATASOURCE), Action.READ)
        )
    );
  }

  @Test
  public void testConfusingViewIdentifiers()
  {
    final String sql = "SELECT COUNT(*) FROM view.dview as druid WHERE druid.numfoo <> 'z'";

    analyzeResources(
        sql,
        ImmutableList.of(
            new ResourceAction(new Resource("dview", ResourceType.VIEW), Action.READ)
        )
    );
  }

  @Test
  public void testDynamicParameters()
  {
    final String sql = "SELECT SUBSTRING(dim2, CAST(? as BIGINT), CAST(? as BIGINT)) FROM druid.foo LIMIT ?";
    analyzeResources(
        sql,
        ImmutableList.of(
            new ResourceAction(new Resource("foo", ResourceType.DATASOURCE), Action.READ)
        )
    );
  }

  @Test
  public void testSysTables()
  {
    testSysTable("SELECT * FROM sys.segments", null, PLANNER_CONFIG_DEFAULT);
    testSysTable("SELECT * FROM sys.servers", null, PLANNER_CONFIG_DEFAULT);
    testSysTable("SELECT * FROM sys.server_segments", null, PLANNER_CONFIG_DEFAULT);
    testSysTable("SELECT * FROM sys.tasks", null, PLANNER_CONFIG_DEFAULT);
    testSysTable("SELECT * FROM sys.supervisors", null, PLANNER_CONFIG_DEFAULT);

    testSysTable("SELECT * FROM sys.segments", "segments", PLANNER_CONFIG_AUTHORIZE_SYS_TABLES);
    testSysTable("SELECT * FROM sys.servers", "servers", PLANNER_CONFIG_AUTHORIZE_SYS_TABLES);
    testSysTable("SELECT * FROM sys.server_segments", "server_segments", PLANNER_CONFIG_AUTHORIZE_SYS_TABLES);
    testSysTable("SELECT * FROM sys.tasks", "tasks", PLANNER_CONFIG_AUTHORIZE_SYS_TABLES);
    testSysTable("SELECT * FROM sys.supervisors", "supervisors", PLANNER_CONFIG_AUTHORIZE_SYS_TABLES);
  }

  private void testSysTable(String sql, String name, PlannerConfig plannerConfig)
  {
    testSysTable(sql, name, ImmutableMap.of(), plannerConfig, new AuthConfig());
  }

  private void testSysTable(
      String sql,
      String name,
      Map<String, Object> context,
      PlannerConfig plannerConfig,
      AuthConfig authConfig
  )
  {
    final List<ResourceAction> expectedResources = new ArrayList<>();
    if (name != null) {
      expectedResources.add(new ResourceAction(new Resource(name, ResourceType.SYSTEM_TABLE), Action.READ));
    }
    if (context != null && !context.isEmpty()) {
      context.forEach((k, v) -> expectedResources.add(
          new ResourceAction(new Resource(k, ResourceType.QUERY_CONTEXT), Action.WRITE)
      ));
    }
    analyzeResources(
        plannerConfig,
        authConfig,
        sql,
        context,
        // Use superuser because, in tests, only the superuser has
        // permission on system tables, and we must do authorization to
        // obtain resources.
        CalciteTests.SUPER_USER_AUTH_RESULT,
        expectedResources
    );
  }

  @Test
  public void testSysTableWithQueryContext()
  {
    final AuthConfig authConfig = AuthConfig.newBuilder().setAuthorizeQueryContextParams(true).build();
    final Map<String, Object> context = ImmutableMap.of(
        "baz", "fo",
        "nested-bar", ImmutableMap.of("nested-key", "nested-val")
    );
    testSysTable("SELECT * FROM sys.segments", null, context, PLANNER_CONFIG_DEFAULT, authConfig);
    testSysTable("SELECT * FROM sys.servers", null, context, PLANNER_CONFIG_DEFAULT, authConfig);
    testSysTable("SELECT * FROM sys.server_segments", null, context, PLANNER_CONFIG_DEFAULT, authConfig);
    testSysTable("SELECT * FROM sys.tasks", null, context, PLANNER_CONFIG_DEFAULT, authConfig);
    testSysTable("SELECT * FROM sys.supervisors", null, context, PLANNER_CONFIG_DEFAULT, authConfig);

    testSysTable("SELECT * FROM sys.segments", "segments", context, PLANNER_CONFIG_AUTHORIZE_SYS_TABLES, authConfig);
    testSysTable("SELECT * FROM sys.servers", "servers", context, PLANNER_CONFIG_AUTHORIZE_SYS_TABLES, authConfig);
    testSysTable(
        "SELECT * FROM sys.server_segments",
        "server_segments",
        context,
        PLANNER_CONFIG_AUTHORIZE_SYS_TABLES,
        authConfig
    );
    testSysTable("SELECT * FROM sys.tasks", "tasks", context, PLANNER_CONFIG_AUTHORIZE_SYS_TABLES, authConfig);
    testSysTable(
        "SELECT * FROM sys.supervisors",
        "supervisors",
        context,
        PLANNER_CONFIG_AUTHORIZE_SYS_TABLES,
        authConfig
    );
  }

  @Test
  public void testQueryContext()
  {
    final String sql = "SELECT COUNT(*) FROM foo WHERE foo.dim1 <> 'z'";
    Map<String, Object> context = ImmutableMap.of("baz", "fo", "nested-bar", ImmutableMap.of("nested-key", "nested-val"));
    analyzeResources(
        PLANNER_CONFIG_DEFAULT,
        AuthConfig.newBuilder().setAuthorizeQueryContextParams(true).build(),
        sql,
        context,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            new ResourceAction(new Resource("foo", ResourceType.DATASOURCE), Action.READ),
            new ResourceAction(new Resource("baz", ResourceType.QUERY_CONTEXT), Action.WRITE),
            new ResourceAction(new Resource("nested-bar", ResourceType.QUERY_CONTEXT), Action.WRITE)
        )
    );
  }
}
