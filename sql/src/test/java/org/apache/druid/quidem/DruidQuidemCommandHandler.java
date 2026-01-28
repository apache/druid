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
import com.google.inject.Injector;
import net.hydromatic.quidem.AbstractCommand;
import net.hydromatic.quidem.Command;
import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem.SqlCommand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Util;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.Query;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assume.assumeFalse;

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
    if (line.startsWith("disabled")) {
      return new DisabledCommand(lines, content, line);
    }
    return null;
  }

  public abstract static class AbstractPlanCommand extends AbstractCommand
  {
    private final List<String> content;
    private final List<String> lines;

    public AbstractPlanCommand(List<String> lines, List<String> content)
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
          // This is packaged as an Error because Quidem grabs anything else and pushes it into the output file
          // believing that you are wanting to validate the error.  By repackaging it, we get a nice failure in the
          // runner instead
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
      try (Closeable unhook = dhp.withHook(hook, (key, value) -> logged.add(value))) {
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

  /**
   * Command that prints the plan for the current query.
   */
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
        String str = convertRelToString(node);
        context.echo(ImmutableList.of(str));
      }
    }

    protected String convertRelToString(RelNode node)
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
    protected String convertRelToString(RelNode node)
    {
      final HintCollector collector = new HintCollector();
      node.accept(collector);
      return collector.getCollectedHintsAsString();
    }

    private static class HintCollector extends RelHomogeneousShuttle
    {
      private final List<String> hintsCollect;

      HintCollector()
      {
        this.hintsCollect = new ArrayList<>();
      }

      @Override
      public RelNode visit(RelNode relNode)
      {
        if (relNode instanceof Hintable) {
          Hintable hintableRelNode = (Hintable) relNode;
          if (!hintableRelNode.getHints().isEmpty()) {
            this.hintsCollect.add(relNode.getClass().getSimpleName() + ":" + hintableRelNode.getHints());
          }
        }
        return super.visit(relNode);
      }

      public String getCollectedHintsAsString()
      {
        StringBuilder builder = new StringBuilder();
        for (String hintLine : hintsCollect) {
          builder.append(hintLine).append("\n");
        }

        return builder.toString();
      }
    }
  }

  /**
   * Usage:
   * <pre>
   * !disabled StandardComponentSupplier reason for disabling
   * </pre>
   */
  static class DisabledCommand extends AbstractCommand
  {
    private String supplierName;
    private String message;
    private Pattern DISABLED_PATTERN = Pattern.compile("disabled\\s+([\\w\\d]+)(\\s+([^\\s].+))?\\s*");

    private final List<String> content;
    private final List<String> lines;

    DisabledCommand(List<String> lines, List<String> content, String line)
    {
      super();
      this.lines = ImmutableList.copyOf(lines);
      this.content = content;
      Matcher m = DISABLED_PATTERN.matcher(line);
      if (!m.matches()) {
        throw new IllegalArgumentException("usage: !disabled <supplier> [<reason>]");
      }
      supplierName = m.group(1);
      message = buildMessage(supplierName, m.group(3));
    }

    private static String buildMessage(String supplierName, String message)
    {
      return StringUtils.format("Test disabled on supplier [%s]. Reason: [%s]", supplierName, message);
    }

    @Override
    public final void execute(Context context, boolean execute)
    {
      if (execute) {
        try {
          Injector injector = DruidConnectionExtras.unwrapOrThrow(context.connection()).getInjector();
          SqlTestFrameworkConfig cfg = injector.getInstance(SqlTestFrameworkConfig.class);
          assumeFalse(message, cfg.componentSupplier.getSimpleName().equals(supplierName));
        }
        catch (Exception e) {
          // This is packaged as an Error because Quidem grabs anything else and pushes it into the output file
          // believing that you are wanting to validate the error.  By repackaging it, we get a nice failure in the
          // runner instead
          throw new Error(e);
        }
      } else {
        context.echo(content);
      }
      context.echo(lines);
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
