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
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.hook.DruidHook;
import org.apache.druid.sql.hook.DruidHook.HookKey;
import org.apache.druid.sql.hook.DruidHookDispatcher;

import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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
      return new DruidPlanCommand(lines, content);
    }
    if (line.startsWith("nativePlan")) {
      return new NativePlanCommand(lines, content);
    }
    if (line.startsWith("msqPlan")) {
      return new MSQPlanCommand(lines, content);
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
    public final String describe(Context ctx)
    {
      return commandName() + " [sql: " + ctx.previousSqlCommand().sql + "]";
    }

    @Override
    public final void execute(Context ctx, boolean execute)
    {
      if (execute) {
        try {
          executeExplain(ctx);
        }
        catch (Exception e) {
          throw new Error(e);
        }
      } else {
        ctx.echo(content);
      }
      ctx.echo(lines);
    }

    protected final void executeQuery(Context ctx)
    {
      final SqlCommand sqlCommand = ctx.previousSqlCommand();
      executeQuery(ctx, sqlCommand.sql);
    }

    protected final void executeExplainQuery(Context ctx)
    {
      boolean isExplainSupported = DruidConnectionExtras.unwrapOrThrow(ctx.connection()).isExplainSupported();

      final SqlCommand sqlCommand = ctx.previousSqlCommand();

      if (isExplainSupported) {
        executeQuery(ctx, "explain plan for " + sqlCommand.sql);
      } else {
        executeQuery(ctx, sqlCommand.sql);
      }
    }

    protected final void executeQuery(Context ctx, String sql)
    {
      try (
          final Statement statement = ctx.connection().createStatement();
          final ResultSet resultSet = statement.executeQuery(sql)) {
        // throw away all results
        while (resultSet.next()) {
          Util.discard(false);
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    protected final DruidHookDispatcher unwrapDruidHookDispatcher(Context ctx)
    {
      return DruidConnectionExtras.unwrapOrThrow(ctx.connection()).getDruidHookDispatcher();
    }

    protected abstract void executeExplain(Context ctx) throws Exception;
  }

  /** Command that prints the plan for the current query. */
  static class NativePlanCommand extends AbstractPlanCommand
  {
    NativePlanCommand(List<String> lines, List<String> content)
    {
      super(lines, content);
    }

    @Override
    protected void executeExplain(Context ctx) throws Exception
    {
      DruidConnectionExtras connectionExtras = (DruidConnectionExtras) ctx.connection();
      ObjectMapper objectMapper = connectionExtras.getObjectMapper();
      DruidHookDispatcher dhp = unwrapDruidHookDispatcher(ctx);
      List<Query<?>> logged = new ArrayList<>();
      try (Closeable unhook = dhp.withHook(DruidHook.NATIVE_PLAN, (key, query) -> {
        logged.add(query);
      })) {
        executeExplainQuery(ctx);
      }

      for (Query<?> query : logged) {
        query = BaseCalciteQueryTest.recursivelyClearContext(query, objectMapper);
        String str = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(query);
        ctx.echo(ImmutableList.of(str));
      }
    }

  }

  /**
   * Handles plan commands captured via {@link Hook}.
   */
  abstract static class AbstractRelPlanCommand extends AbstractPlanCommand
  {
    HookKey<RelNode> hook;

    AbstractRelPlanCommand(List<String> lines, List<String> content, DruidHook.HookKey<RelNode> hook)
    {
      super(lines, content);
      this.hook = hook;
    }

    @Override
    protected final void executeExplain(Context ctx) throws IOException
    {
      DruidHookDispatcher dhp = unwrapDruidHookDispatcher(ctx);
      List<RelNode> logged = new ArrayList<>();
      try (Closeable unhook = dhp.withHook(hook, (key, relNode) -> {
        logged.add(relNode);
      })) {
        executeExplainQuery(ctx);
      }

      for (RelNode node : logged) {
        if (node instanceof DruidRel<?>) {
          node = ((DruidRel) node).unwrapLogicalPlan();
        }
        String str = RelOptUtil.dumpPlan("", node, SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
        ctx.echo(ImmutableList.of(str));
      }
    }
  }

  /**
   * Handles plan commands captured via {@link Hook}.
   */
  abstract static class AbstractStringCaptureCommand extends AbstractPlanCommand
  {
    HookKey<String> hook;

    AbstractStringCaptureCommand(List<String> lines, List<String> content, DruidHook.HookKey<String> hook)
    {
      super(lines, content);
      this.hook = hook;
    }

    @Override
    protected final void executeExplain(Context ctx) throws IOException
    {
      DruidHookDispatcher dhp = unwrapDruidHookDispatcher(ctx);
      List<String> logged = new ArrayList<>();
      try (Closeable unhook = dhp.withHook(hook, (key, relNode) -> {
        logged.add(relNode);
      })) {
        executeQuery(ctx);
      }

      ctx.echo(logged);
    }
  }

  static class LogicalPlanCommand extends AbstractRelPlanCommand
  {
    LogicalPlanCommand(List<String> lines, List<String> content)
    {
      super(lines, content, DruidHook.LOGICAL_PLAN);
    }
  }

  static class DruidPlanCommand extends AbstractRelPlanCommand
  {
    DruidPlanCommand(List<String> lines, List<String> content)
    {
      super(lines, content, DruidHook.DRUID_PLAN);
    }
  }

  static class ConvertedPlanCommand extends AbstractRelPlanCommand
  {
    ConvertedPlanCommand(List<String> lines, List<String> content)
    {
      super(lines, content, DruidHook.CONVERTED_PLAN);
    }
  }
  static class MSQPlanCommand extends AbstractStringCaptureCommand
  {
    MSQPlanCommand(List<String> lines, List<String> content)
    {
      super(lines, content, DruidHook.MSQ_PLAN);
    }
  }
}
