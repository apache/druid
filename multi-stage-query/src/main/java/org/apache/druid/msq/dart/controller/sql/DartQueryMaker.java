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

package org.apache.druid.msq.dart.controller.sql;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.frame.Frame;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.io.LimitedOutputStream;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.msq.dart.controller.ControllerHolder;
import org.apache.druid.msq.dart.controller.ControllerThreadPool;
import org.apache.druid.msq.dart.controller.DartControllerContextFactory;
import org.apache.druid.msq.dart.controller.DartControllerRegistry;
import org.apache.druid.msq.dart.guice.DartControllerConfig;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.exec.ControllerImpl;
import org.apache.druid.msq.exec.QueryKitSpecFactory;
import org.apache.druid.msq.exec.ResultsContext;
import org.apache.druid.msq.exec.SequenceQueryListener;
import org.apache.druid.msq.indexing.LegacyMSQSpec;
import org.apache.druid.msq.indexing.QueryDefMSQSpec;
import org.apache.druid.msq.indexing.TaskReportQueryListener;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.error.CancellationReason;
import org.apache.druid.msq.querykit.MultiQueryKit;
import org.apache.druid.msq.sql.MSQTaskQueryMaker;
import org.apache.druid.msq.util.SqlStatementResourceHelper;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.QueryUtils;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlResults;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * SQL {@link QueryMaker}. Executes queries in two ways, depending on whether the user asked for a full report.
 *
 * When including a full report, the entire response is buffered in memory, up to
 * {@link DartControllerConfig#getMaxQueryReportSize()}.
 *
 * When not including a full report, results are streamed back to the user through a {@link SequenceQueryListener}.
 * There is no limit to the size of the returned results.
 */
public class DartQueryMaker implements QueryMaker
{
  final List<Entry<Integer, String>> fieldMapping;
  private final DartControllerContextFactory controllerContextFactory;
  private final PlannerContext plannerContext;

  /**
   * Controller registry, used to register and remove controllers as they start and finish.
   */
  private final DartControllerRegistry controllerRegistry;

  /**
   * Controller config.
   */
  private final DartControllerConfig controllerConfig;

  /**
   * Executor for running controllers.
   */
  private final ControllerThreadPool controllerThreadPool;
  private final ServerConfig serverConfig;

  final QueryKitSpecFactory queryKitSpecFactory;
  final MultiQueryKit queryKit;

  public DartQueryMaker(
      List<Entry<Integer, String>> fieldMapping,
      DartControllerContextFactory controllerContextFactory,
      PlannerContext plannerContext,
      DartControllerRegistry controllerRegistry,
      DartControllerConfig controllerConfig,
      ControllerThreadPool controllerThreadPool,
      QueryKitSpecFactory queryKitSpecFactory,
      MultiQueryKit queryKit,
      ServerConfig serverConfig
  )
  {
    this.fieldMapping = fieldMapping;
    this.controllerContextFactory = controllerContextFactory;
    this.plannerContext = plannerContext;
    this.controllerRegistry = controllerRegistry;
    this.controllerConfig = controllerConfig;
    this.controllerThreadPool = controllerThreadPool;
    this.queryKitSpecFactory = queryKitSpecFactory;
    this.queryKit = queryKit;
    this.serverConfig = serverConfig;
  }

  @Override
  public QueryResponse<Object[]> runQuery(DruidQuery druidQuery)
  {
    ColumnMappings columnMappings = QueryUtils.buildColumnMappings(fieldMapping, druidQuery.getOutputRowSignature());
    final LegacyMSQSpec querySpec = MSQTaskQueryMaker.makeLegacyMSQSpec(
        null,
        druidQuery,
        finalizeTimeout(druidQuery.getQuery().context()),
        columnMappings,
        plannerContext,
        null
    );


    final ResultsContext resultsContext = MSQTaskQueryMaker.makeResultsContext(druidQuery, fieldMapping, plannerContext);

    return runLegacyMSQSpec(querySpec, druidQuery.getQuery().context(), resultsContext);
  }

  public static ResultsContext makeResultsContext(
      DruidQuery druidQuery,
      List<Entry<Integer, String>> fieldMapping,
      PlannerContext plannerContext
  )
  {
    final List<Pair<SqlTypeName, ColumnType>> types = MSQTaskQueryMaker.getTypes(druidQuery, fieldMapping, plannerContext);
    final ResultsContext resultsContext = new ResultsContext(
        types.stream().map(p -> p.lhs).collect(Collectors.toList()),
        SqlResults.Context.fromPlannerContext(plannerContext)
    );
    return resultsContext;
  }

  /**
   * Creates a controller using {@link LegacyMSQSpec} and calls {@link #runController}.
   */
  public QueryResponse<Object[]> runLegacyMSQSpec(
      LegacyMSQSpec querySpec,
      QueryContext context,
      ResultsContext resultsContext
  )
  {
    final ControllerImpl controller = makeLegacyController(querySpec, context, resultsContext);
    return runController(controller, context.getFullReport(), querySpec.getColumnMappings(), resultsContext);
  }

  /**
   * Creates a controller using {@link QueryDefMSQSpec} and calls {@link #runController}.
   */
  public QueryResponse<Object[]> runQueryDefMSQSpec(
      QueryDefMSQSpec querySpec,
      QueryContext context,
      ResultsContext resultsContext
  )
  {
    final ControllerImpl controller = makeQueryDefController(querySpec, context, resultsContext);
    return runController(controller, context.getFullReport(), querySpec.getColumnMappings(), resultsContext);
  }

  private ControllerImpl makeLegacyController(LegacyMSQSpec querySpec, QueryContext context, ResultsContext resultsContext)
  {
    final ControllerContext controllerContext = controllerContextFactory.newContext(context);

    return new ControllerImpl(
        querySpec,
        resultsContext,
        controllerContext,
        queryKitSpecFactory
    );
  }

  private ControllerImpl makeQueryDefController(QueryDefMSQSpec querySpec, QueryContext context, ResultsContext resultsContext)
  {
    final ControllerContext controllerContext = controllerContextFactory.newContext(context);

    return new ControllerImpl(
        querySpec,
        resultsContext,
        controllerContext,
        queryKitSpecFactory
    );
  }

  /**
   * Adds the timeout parameter to the query context, considering the default and maximum values from
   * {@link ServerConfig}.
   */
  private QueryContext finalizeTimeout(QueryContext queryContext)
  {
    final long timeout = queryContext.getTimeout(serverConfig.getDefaultQueryTimeout());
    QueryContext timeoutContext = queryContext.override(Map.of(QueryContexts.TIMEOUT_KEY, timeout));
    timeoutContext.verifyMaxQueryTimeout(serverConfig.getMaxQueryTimeout());
    return timeoutContext;
  }

  /**
   * Runs a controller in {@link #controllerThreadPool} and returns a {@link QueryResponse} object.
   *
   * @param controller     controller to run
   * @param fullReport     if true, buffer the results into a report and return it in a single row.
   *                       if false, stream the results back
   * @param columnMappings SQL column mappings
   * @param resultsContext SQL results context
   */
  private QueryResponse<Object[]> runController(
      final ControllerImpl controller,
      final boolean fullReport,
      final ColumnMappings columnMappings,
      final ResultsContext resultsContext
  )
  {
    final ControllerHolder controllerHolder = new ControllerHolder(
        controller,
        plannerContext.getSqlQueryId(),
        plannerContext.getSql(),
        plannerContext.getAuthenticationResult(),
        DateTimes.nowUtc()
    );

    // runWithReport, runWithoutReport are responsible for calling controllerRegistry.deregister(controllerHolder)
    // when their work is done.
    final Sequence<Object[]> results =
        fullReport
        ? runWithReport(controllerHolder, columnMappings, resultsContext)
        : runWithSequence(controllerHolder, columnMappings, resultsContext);
    return QueryResponse.withEmptyContext(results);
  }

  /**
   * Run a query and return the full report, buffered in memory up to
   * {@link DartControllerConfig#getMaxQueryReportSize()}.
   *
   * Arranges for {@link DartControllerRegistry#deregister} to be called upon completion (either success or failure).
   */
  private Sequence<Object[]> runWithReport(
      final ControllerHolder controllerHolder,
      final ColumnMappings columnMappings,
      final ResultsContext resultsContext
  )
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final TaskReportQueryListener listener = new TaskReportQueryListener(
        () -> new LimitedOutputStream(
            baos,
            controllerConfig.getMaxQueryReportSize(),
            limit -> StringUtils.format(
                "maxQueryReportSize[%,d] exceeded. "
                + "Try limiting the result set for your query, or run it with %s[false]",
                limit,
                QueryContexts.CTX_FULL_REPORT
            )
        ),
        plannerContext.getJsonMapper(),
        controllerHolder.getController().queryId(),
        Collections.emptyMap(),
        MSQDestination.UNLIMITED,
        columnMappings,
        resultsContext
    );

    try {
      // Submit controller and wait for it to finish.
      controllerHolder.runAsync(listener, controllerRegistry, controllerThreadPool).get();

      // Return a sequence with just one row (the report).
      final TaskReport.ReportMap reportMap =
          plannerContext.getJsonMapper().readValue(baos.toByteArray(), TaskReport.ReportMap.class);
      return Sequences.simple(List.<Object[]>of(new Object[]{reportMap}));
    }
    catch (InterruptedException e) {
      controllerHolder.cancel(CancellationReason.UNKNOWN);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Run a query and return the results only, streamed back using {@link SequenceQueryListener}.
   *
   * Arranges for {@link DartControllerRegistry#deregister} to be called upon completion (either success or failure).
   */
  private Sequence<Object[]> runWithSequence(
      final ControllerHolder controllerHolder,
      final ColumnMappings columnMappings,
      final ResultsContext resultsContext
  )
  {
    final SequenceQueryListener listener = new SequenceQueryListener();
    final ListenableFuture<?> runFuture =
        controllerHolder.runAsync(listener, controllerRegistry, controllerThreadPool);

    return Sequences.wrap(
        listener.getSequence().flatMap(
            rac -> SqlStatementResourceHelper.getResultSequence(
                rac.as(Frame.class),
                listener.getFrameReader(),
                columnMappings,
                resultsContext,
                plannerContext.getJsonMapper()
            )
        ),
        new SequenceWrapper()
        {
          @Override
          public void after(final boolean isDone, final Throwable thrown)
          {
            if (!isDone || thrown != null) {
              runFuture.cancel(true); // Cancel on early stop or failure
            }
          }
        }
    );
  }
}
