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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.io.LimitedOutputStream;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.dart.controller.ControllerHolder;
import org.apache.druid.msq.dart.controller.DartControllerRegistry;
import org.apache.druid.msq.dart.guice.DartControllerConfig;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.exec.ControllerImpl;
import org.apache.druid.msq.exec.QueryListener;
import org.apache.druid.msq.exec.ResultsContext;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.TaskReportQueryListener;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.sql.MSQTaskQueryMaker;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlResults;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * SQL {@link QueryMaker}. Executes queries in two ways, depending on whether the user asked for a full report.
 *
 * When including a full report, the controller runs in the SQL planning thread (typically an HTTP thread) using
 * the method {@link #runWithReport(ControllerHolder)}. The entire response is buffered in memory, up to
 * {@link DartControllerConfig#getMaxQueryReportSize()}.
 *
 * When not including a full report, the controller runs in {@link #controllerExecutor} and results are streamed
 * back to the user through {@link ResultIterator}. There is no limit to the size of the returned results.
 */
public class DartQueryMaker implements QueryMaker
{
  private static final Logger log = new Logger(DartQueryMaker.class);

  private final List<Entry<Integer, String>> fieldMapping;
  private final ControllerContext controllerContext;
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
   * Executor for {@link #runWithoutReport(ControllerHolder)}. Number of thread is equal to
   * {@link DartControllerConfig#getConcurrentQueries()}, which limits the number of concurrent controllers.
   */
  private final ExecutorService controllerExecutor;

  public DartQueryMaker(
      List<Entry<Integer, String>> fieldMapping,
      ControllerContext controllerContext,
      PlannerContext plannerContext,
      DartControllerRegistry controllerRegistry,
      DartControllerConfig controllerConfig,
      ExecutorService controllerExecutor
  )
  {
    this.fieldMapping = fieldMapping;
    this.controllerContext = controllerContext;
    this.plannerContext = plannerContext;
    this.controllerRegistry = controllerRegistry;
    this.controllerConfig = controllerConfig;
    this.controllerExecutor = controllerExecutor;
  }

  @Override
  public QueryResponse<Object[]> runQuery(DruidQuery druidQuery)
  {
    final MSQSpec querySpec = MSQTaskQueryMaker.makeQuerySpec(
        null,
        druidQuery,
        fieldMapping,
        plannerContext,
        null // Only used for DML, which this isn't
    );
    final List<Pair<SqlTypeName, ColumnType>> types =
        MSQTaskQueryMaker.getTypes(druidQuery, fieldMapping, plannerContext);

    final ControllerImpl controller = new ControllerImpl(
        druidQuery.getQuery().context().getString(DartSqlEngine.CTX_DART_QUERY_ID),
        querySpec,
        new ResultsContext(
            types.stream().map(p -> p.lhs).collect(Collectors.toList()),
            SqlResults.Context.fromPlannerContext(plannerContext)
        ),
        controllerContext
    );

    final ControllerHolder controllerHolder = new ControllerHolder(
        controller,
        plannerContext.getSqlQueryId(),
        plannerContext.getSql(),
        plannerContext.getAuthenticationResult(),
        DateTimes.nowUtc()
    );

    final boolean fullReport = druidQuery.getQuery().context().getBoolean(
        DartSqlEngine.CTX_FULL_REPORT,
        DartSqlEngine.CTX_FULL_REPORT_DEFAULT
    );

    // Register controller before acquiring the semaphore, so it shows up in "active controllers" lists.
    controllerRegistry.register(controllerHolder);

    try {
      final Sequence<Object[]> results =
          fullReport ? runWithReport(controllerHolder) : runWithoutReport(controllerHolder);
      return QueryResponse.withEmptyContext(results);
    }
    catch (Throwable e) {
      // Error while creating the result sequence; unregister controller.
      controllerRegistry.remove(controllerHolder);
      throw e;
    }
  }

  /**
   * Run a query and return the full report, buffered in memory up to
   * {@link DartControllerConfig#getMaxQueryReportSize()}.
   */
  private Sequence<Object[]> runWithReport(final ControllerHolder controllerHolder)
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final TaskReportQueryListener queryListener = new TaskReportQueryListener(
        TaskReportMSQDestination.instance(),
        () -> new LimitedOutputStream(
            baos,
            controllerConfig.getMaxQueryReportSize(),
            limit -> StringUtils.format(
                "maxQueryReportSize[%,d] exceeded. "
                + "Try limiting the result set for your query, or run it with %s[false]",
                limit,
                DartSqlEngine.CTX_FULL_REPORT
            )
        ),
        plannerContext.getJsonMapper(),
        controllerHolder.getController().queryId(),
        Collections.emptyMap()
    );

    try {
      controllerHolder.getController().run(queryListener);

      final Map<String, Object> reportAsMap =
          plannerContext.getJsonMapper().readValue(baos.toByteArray(), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
      return Sequences.simple(Collections.singletonList(new Object[]{reportAsMap}));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    finally {
      controllerRegistry.remove(controllerHolder);
    }
  }

  /**
   * Run a query and return the results only, streamed back using {@link ResultIteratorMaker}.
   */
  private Sequence<Object[]> runWithoutReport(final ControllerHolder controllerHolder)
  {
    return new BaseSequence<>(new ResultIteratorMaker(controllerHolder));
  }

  /**
   * Helper for {@link #runWithoutReport(ControllerHolder)}.
   */
  class ResultIteratorMaker implements BaseSequence.IteratorMaker<Object[], ResultIterator>
  {
    private final ControllerHolder controllerHolder;

    public ResultIteratorMaker(ControllerHolder holder)
    {
      this.controllerHolder = holder;
    }

    @Override
    public ResultIterator make()
    {
      final ResultIterator iter = new ResultIterator();

      // This separate thread is needed because we need to return a Sequence that streams results. If the
      // QueryMaker interface was changed to use an object that pushes out results rather than returning a Sequence,
      // then we wouldn't need this separate thread.
      final Future<?> controllerFuture = controllerExecutor.submit(() -> {
        final Controller controller = controllerHolder.getController();
        final String threadName = Thread.currentThread().getName();

        try {
          Thread.currentThread().setName(
              StringUtils.format(
                  "%s-sqlQueryId[%s]-queryId[%s]",
                  threadName,
                  plannerContext.getSqlQueryId(),
                  controller.queryId()
              )
          );
          controller.run(iter);
        }
        catch (Exception e) {
          log.warn(
              e,
              "Controller failed for sqlQueryId[%s], controllerHost[%s]",
              plannerContext.getSqlQueryId(),
              controller.queryId()
          );
        }
        finally {
          controllerRegistry.remove(controllerHolder);
          Thread.currentThread().setName(threadName);
        }
      });

      iter.attach(() -> {
        controllerFuture.cancel(false);

        if (!iter.complete) {
          controllerHolder.getController().stop();
        }
      });

      return iter;
    }

    @Override
    public void cleanup(final ResultIterator iterFromMake)
    {
      CloseableUtils.closeAndWrapExceptions(iterFromMake);
    }
  }

  /**
   * Helper for {@link ResultIteratorMaker}, which is in turn a helper for {@link #runWithoutReport(ControllerHolder)}.
   */
  static class ResultIterator implements Iterator<Object[]>, QueryListener, Closeable
  {
    /**
     * Number of rows to buffer from {@link #onResultRow(Object[])}.
     */
    private static final int BUFFER_SIZE = 128;

    /**
     * Empty optional signifies results are complete.
     */
    private final BlockingQueue<Either<Throwable, Object[]>> rowBuffer = new ArrayBlockingQueue<>(BUFFER_SIZE);

    /**
     * Only accessed by {@link Iterator} methods, so no need to be thread-safe.
     */
    @Nullable
    private Either<Throwable, Object[]> current;

    @GuardedBy("closer")
    private final Closer closer = Closer.create();

    private volatile boolean complete;

    @Override
    public boolean hasNext()
    {
      return populateAndReturnCurrent().isPresent();
    }

    @Override
    public Object[] next()
    {
      final Object[] retVal = populateAndReturnCurrent().orElseThrow(NoSuchElementException::new);
      current = null;
      return retVal;
    }

    private Optional<Object[]> populateAndReturnCurrent()
    {
      if (current == null) {
        try {
          current = rowBuffer.take();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }

      return Optional.ofNullable(current.valueOrThrow());
    }

    @Override
    public boolean readResults()
    {
      return !complete;
    }

    @Override
    public void onResultsStart(
        final List<MSQResultsReport.ColumnAndType> signature,
        @Nullable final List<SqlTypeName> sqlTypeNames
    )
    {
      // Nothing to do.
    }

    @Override
    public boolean onResultRow(Object[] row)
    {
      try {
        rowBuffer.put(Either.value(row));
        return !complete;
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onResultsComplete()
    {
      // Nothing to do.
    }

    @Override
    public void onQueryComplete(MSQTaskReportPayload report)
    {
      try {
        complete = true;

        final MSQStatusReport statusReport = report.getStatus();

        if (statusReport.getStatus().isSuccess()) {
          rowBuffer.put(Either.value(null));
        } else {
          rowBuffer.put(Either.error(new MSQException(statusReport.getErrorReport().getFault())));
        }
      }
      catch (InterruptedException e) {
        // Can't fix this by putting an error, because the rowBuffer isn't accepting new entries.
        // Give up, allow controller.run() to fail.
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    private void attach(final Closeable closeable)
    {
      synchronized (closer) {
        closer.register(closeable);
      }
    }

    @Override
    public void close() throws IOException
    {
      synchronized (closer) {
        closer.close();
      }
    }
  }
}
