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
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
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
    if (line.startsWith("hints")) {
      return new HintPlanCommand(lines, content);
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
    public final String describe(Context context)
    {
      return commandName() + " [sql: " + context.previousSqlCommand().sql + "]";
    }

    @Override
    public final void execute(Context context, boolean execute)
    {
      if (execute) {
        try {
          executeExplain(context);
        }
        catch (Exception e) {
          throw new Error(e);
        }
      } else {
        context.echo(content);
      }
      context.echo(lines);
    }

    protected final <T> List<T> executeExplainCollectHookValues(Context context, HookKey<T> hook) throws IOException
    {
      DruidHookDispatcher dhp = unwrapDruidHookDispatcher(context);
      List<T> logged = new ArrayList<>();
      try (Closeable unhook = dhp.withHook(hook, (key, value) -> {
        logged.add(value);
      })) {
        executeExplainQuery(context);
      }
      return logged;
    }

    protected final void executeQuery(Context context)
    {
      final SqlCommand sqlCommand = context.previousSqlCommand();
      executeQuery(context, sqlCommand.sql);
    }

    protected final void executeExplainQuery(Context context)
    {
      boolean isExplainSupported = DruidConnectionExtras.unwrapOrThrow(context.connection()).isExplainSupported();

      final SqlCommand sqlCommand = context.previousSqlCommand();

      if (isExplainSupported) {
        executeQuery(context, "explain plan for " + sqlCommand.sql);
      } else {
        executeQuery(context, sqlCommand.sql);
      }
    }

    protected final void executeQuery(Context context, String sql)
    {
      try (
          final Statement statement = context.connection().createStatement();
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

    protected final DruidHookDispatcher unwrapDruidHookDispatcher(Context context)
    {
      return DruidConnectionExtras.unwrapOrThrow(context.connection()).getDruidHookDispatcher();
    }

    protected abstract void executeExplain(Context context) throws Exception;
  }

  /** Command that prints the plan for the current query. */
  static class NativePlanCommand extends AbstractPlanCommand
  {
    NativePlanCommand(List<String> lines, List<String> content)
    {
      super(lines, content);
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected void executeExplain(Context context) throws Exception
    {
      DruidConnectionExtras connectionExtras = DruidConnectionExtras.unwrapOrThrow(context.connection());
      ObjectMapper objectMapper = connectionExtras.getObjectMapper();

      List<Query> logged = executeExplainCollectHookValues(context, DruidHook.NATIVE_PLAN);

      for (Query<?> query : logged) {
        query = BaseCalciteQueryTest.recursivelyClearContext(query, objectMapper);
        String str = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(query);
        context.echo(ImmutableList.of(str));
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
    protected final void executeExplain(Context context) throws IOException
    {
      List<RelNode> logged = executeExplainCollectHookValues(context, hook);

      for (RelNode node : logged) {
        if (node instanceof DruidRel<?>) {
          node = ((DruidRel<?>) node).unwrapLogicalPlan();
        }
        String str = getString(node);
        context.echo(ImmutableList.of(str));
      }
    }

    protected String getString(RelNode node)
    {
      String str = RelOptUtil.dumpPlan("", node, SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
      return str;
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
    protected final void executeExplain(Context context) throws IOException
    {
      List<String> logged = executeExplainCollectHookValues(context, hook);
      context.echo(logged);
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

  static class HintPlanCommand extends AbstractRelPlanCommand
  {
    HintPlanCommand(List<String> lines, List<String> content)
    {
      super(lines, content, DruidHook.DRUID_PLAN);
    }

    @Override
    protected String getString(RelNode node)
    {
      final List<String> hintsCollect = new ArrayList<>();
      final HintCollector collector = new HintCollector(hintsCollect);
      node.accept(collector);
      StringBuilder builder = new StringBuilder();
      for (String hintLine : hintsCollect) {
        builder.append(hintLine).append("\n");
      }

      return builder.toString();
    }

    private static class HintCollector extends RelShuttleImpl
    {
      private final List<String> hintsCollect;

      HintCollector(List<String> hintsCollect)
      {
        this.hintsCollect = hintsCollect;
      }

      @Override
      public RelNode visit(TableScan scan)
      {
        if (!scan.getHints().isEmpty()) {
          this.hintsCollect.add("TableScan:" + scan.getHints());
        }
        return super.visit(scan);
      }

      @Override
      public RelNode visit(LogicalJoin join)
      {
        if (!join.getHints().isEmpty()) {
          this.hintsCollect.add("LogicalJoin:" + join.getHints());
        }
        return super.visit(join);
      }

      @Override
      public RelNode visit(LogicalProject project)
      {
        if (!project.getHints().isEmpty()) {
          this.hintsCollect.add("Project:" + project.getHints());
        }
        return super.visit(project);
      }

      @Override
      public RelNode visit(LogicalAggregate aggregate)
      {
        if (!aggregate.getHints().isEmpty()) {
          this.hintsCollect.add("Aggregate:" + aggregate.getHints());
        }
        return super.visit(aggregate);
      }

      @Override
      public RelNode visit(LogicalCorrelate correlate)
      {
        if (!correlate.getHints().isEmpty()) {
          this.hintsCollect.add("Correlate:" + correlate.getHints());
        }
        return super.visit(correlate);
      }

      @Override
      public RelNode visit(LogicalFilter filter)
      {
        if (!filter.getHints().isEmpty()) {
          this.hintsCollect.add("Filter:" + filter.getHints());
        }
        return super.visit(filter);
      }

      @Override
      public RelNode visit(LogicalUnion union)
      {
        if (!union.getHints().isEmpty()) {
          this.hintsCollect.add("Union:" + union.getHints());
        }
        return super.visit(union);
      }

      @Override
      public RelNode visit(LogicalIntersect intersect)
      {
        if (!intersect.getHints().isEmpty()) {
          this.hintsCollect.add("Intersect:" + intersect.getHints());
        }
        return super.visit(intersect);
      }

      @Override
      public RelNode visit(LogicalMinus minus)
      {
        if (!minus.getHints().isEmpty()) {
          this.hintsCollect.add("Minus:" + minus.getHints());
        }
        return super.visit(minus);
      }

      @Override
      public RelNode visit(LogicalSort sort)
      {
        if (!sort.getHints().isEmpty()) {
          this.hintsCollect.add("Sort:" + sort.getHints());
        }
        return super.visit(sort);
      }

      @Override
      public RelNode visit(LogicalValues values)
      {
        if (!values.getHints().isEmpty()) {
          this.hintsCollect.add("Values:" + values.getHints());
        }
        return super.visit(values);
      }

      @Override
      public RelNode visit(RelNode other)
      {
        if (other instanceof Window) {
          Window window = (Window) other;
          if (!window.getHints().isEmpty()) {
            this.hintsCollect.add("Window:" + window.getHints());
          }
        } else if (other instanceof Snapshot) {
          Snapshot snapshot = (Snapshot) other;
          if (!snapshot.getHints().isEmpty()) {
            this.hintsCollect.add("Snapshot:" + snapshot.getHints());
          }
        } else if (other instanceof TableFunctionScan) {
          TableFunctionScan scan = (TableFunctionScan) other;
          if (!scan.getHints().isEmpty()) {
            this.hintsCollect.add("TableFunctionScan:" + scan.getHints());
          }
        }
        return super.visit(other);
      }
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
