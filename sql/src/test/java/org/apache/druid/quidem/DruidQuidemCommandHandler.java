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

package org.apache.druid.quidem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import net.hydromatic.quidem.AbstractCommand;
import net.hydromatic.quidem.Command;
import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem.SqlCommand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Util;
import org.apache.druid.query.Query;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.util.QueryLogHook;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DruidQuidemCommandHandler implements CommandHandler
{

  @Override
  public Command parseCommand(List<String> lines, List<String> content, String line)
  {
    if (line.startsWith("convertedPlan")) {
      return new ConvertedPlanCommand(lines, content);
    }
    if (line.startsWith("logicalPlan")) {
      return new LogicalPlanCommand(lines, content);
    }
    if (line.startsWith("druidPlan")) {
      return new PhysicalPlanCommand(lines, content);
    }
    if (line.startsWith("nativePlan")) {
      return new NativePlanCommand(lines, content);
    }
    return null;
  }

  abstract static class AbstractPlanCommand extends AbstractCommand
  {
    private final List<String> content;
    private final List<String> lines;

    AbstractPlanCommand(List<String> lines, List<String> content)
    {
      this.lines = ImmutableList.copyOf(lines);
      this.content = content;
    }

    @Override
    public final String describe(Context x)
    {
      return commandName() + " [sql: " + x.previousSqlCommand().sql + "]";
    }

    @Override
    public final void execute(Context x, boolean execute)
    {
      if (execute) {
        try {
          executeExplain(x);
        }
        catch (Exception e) {
          throw new Error(e);
        }
      } else {
        x.echo(content);
      }
      x.echo(lines);
    }

    protected final void executeQuery(Context x)
    {
      final SqlCommand sqlCommand = x.previousSqlCommand();
      try (
          final Statement statement = x.connection().createStatement();
          final ResultSet resultSet = statement.executeQuery(sqlCommand.sql)) {
        // throw away all results
        while (resultSet.next()) {
          Util.discard(false);
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    protected abstract void executeExplain(Context x) throws Exception;
  }

  /** Command that prints the plan for the current query. */
  static class NativePlanCommand extends AbstractPlanCommand
  {
    NativePlanCommand(List<String> lines, List<String> content)
    {
      super(lines, content);
    }

    @Override
    protected void executeExplain(Context x) throws Exception
    {
      DruidConnectionExtras connectionExtras = (DruidConnectionExtras) x.connection();
      ObjectMapper objectMapper = connectionExtras.getObjectMapper();
      QueryLogHook qlh = new QueryLogHook(objectMapper);
      qlh.logQueriesForGlobal(
          () -> {
            executeQuery(x);
          }
      );

      List<Query<?>> queries = qlh.getRecordedQueries();

      queries = queries
          .stream()
          .map(q -> BaseCalciteQueryTest.recursivelyClearContext(q, objectMapper))
          .collect(Collectors.toList());

      for (Query<?> query : queries) {
        String str = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(query);
        x.echo(ImmutableList.of(str));
      }
    }
  }

  /**
   * Handles plan commands captured via {@link Hook}.
   */
  abstract static class AbstractRelPlanCommand extends AbstractPlanCommand
  {
    Hook hook;

    AbstractRelPlanCommand(List<String> lines, List<String> content, Hook hook)
    {
      super(lines, content);
      this.hook = hook;
    }

    @Override
    protected final void executeExplain(Context x)
    {
      List<RelNode> logged = new ArrayList<>();
      try (final Hook.Closeable unhook = hook.add((Consumer<RelNode>) logged::add)) {
        executeQuery(x);
      }

      for (RelNode node : logged) {
        String str = RelOptUtil.dumpPlan("", node, SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
        x.echo(ImmutableList.of(str));
      }
    }
  }

  static class LogicalPlanCommand extends AbstractRelPlanCommand
  {
    LogicalPlanCommand(List<String> lines, List<String> content)
    {
      super(lines, content, Hook.TRIMMED);
    }
  }

  static class PhysicalPlanCommand extends AbstractRelPlanCommand
  {
    PhysicalPlanCommand(List<String> lines, List<String> content)
    {
      super(lines, content, Hook.JAVA_PLAN);
    }
  }

  static class ConvertedPlanCommand extends AbstractRelPlanCommand
  {
    ConvertedPlanCommand(List<String> lines, List<String> content)
    {
      super(lines, content, Hook.CONVERTED);
    }
  }
}
