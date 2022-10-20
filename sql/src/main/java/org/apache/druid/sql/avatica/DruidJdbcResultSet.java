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

package org.apache.druid.sql.avatica;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.calcite.avatica.Meta;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.DirectStatement;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Druid's server-side representation of a JDBC result set. At most one
 * can be open per statement (standard or prepared). The implementation
 * is based on Druid's own {@link DirectStatement} class. Even if result
 * set is for a {@code PreparedStatement}, the result set itself uses
 * a Druid {@code DirectStatement} which includes the parameter values
 * given for the execution. This allows Druid's planner to use the "query
 * optimized" form of parameter substitution: we replan the query for
 * each execution with the parameter values.
 * <p>
 * Avatica returns results in {@link Meta.Frame} objects as batches of
 * rows. The result set uses the {@code TYPE_FORWARD_ONLY} execution model:
 * the application can only read results sequentially, the application
 * can't jump around or read backwards. As a result, the enclosing
 * statement closes the result set at EOF to release resources early.
 * <p>
 * Thread safe, but only when accessed sequentially by different request
 * threads: not designed for concurrent access as JDBC does not support
 * concurrent actions on the same result set.
 */
public class DruidJdbcResultSet implements Closeable
{
  private static final Logger LOG = new Logger(DruidJdbcResultSet.class);

  /**
   * Asynchronous result fetcher. JDBC operates via REST, which is subject to
   * a timeout if a query takes too long to respond. Fortunately, JDBC uses a
   * batched API, and is perfectly happy to get an empty batch. This class
   * runs in a separate thread to fetch a batch. If the fetch takes too long,
   * the JDBC request thread will time out waiting, will return an empty batch
   * to the client, and will remember the fetch for use in the next fetch
   * request. The result is that the time it takes to produce results for long
   * running queries is decoupled from the HTTP timeout.
   */
  public static class ResultFetcher implements Callable<Meta.Frame>
  {
    private final int limit;
    private int batchSize;
    private int offset;
    private Yielder<Object[]> yielder;

    public ResultFetcher(
        final int limit,
        final Yielder<Object[]> yielder
    )
    {
      this.limit = limit;
      this.yielder = yielder;
    }

    /**
     * In an ideal world, the batch size would be a constructor parameter. But, JDBC,
     * oddly, allows a different batch size per request. Hence, we set the size using
     * this method before each fetch attempt.
     */
    public void setBatchSize(int batchSize)
    {
      this.batchSize = batchSize;
    }

    /**
     * Result is only valid between executions, which turns out to be
     * the only time it is called.
     */
    public int offset()
    {
      return offset;
    }

    /**
     * Fetch the next batch up to the batch size or EOF. Return
     * the resulting frame. Exceptions are handled by the executor
     * framework.
     */
    @Override
    public Meta.Frame call()
    {
      Preconditions.checkState(batchSize > 0);
      int rowCount = 0;
      final int batchLimit = Math.min(limit - offset, batchSize);
      final List<Object> rows = new ArrayList<>(batchLimit);
      while (!yielder.isDone() && rowCount < batchLimit) {
        rows.add(yielder.get());
        yielder = yielder.next(null);
        rowCount++;
      }

      final Meta.Frame result = new Meta.Frame(offset, yielder.isDone(), rows);
      offset += rowCount;
      return result;
    }
  }

  /**
   * Creates the result fetcher and holds config. Rather overkill for production,
   * but handy for testing.
   */
  public static class ResultFetcherFactory
  {
    final int fetchTimeoutMs;

    public ResultFetcherFactory(int fetchTimeoutMs)
    {
      // To prevent server hammering, the timeout must be at least 1 second.
      this.fetchTimeoutMs = Math.max(1000, fetchTimeoutMs);
    }

    public int fetchTimeoutMs()
    {
      return fetchTimeoutMs;
    }

    public ResultFetcher newFetcher(
        final int limit,
        final Yielder<Object[]> yielder
    )
    {
      return new ResultFetcher(limit, yielder);
    }
  }

  /**
   * Query metrics can only be used within a single thread. Because results can
   * be paginated into multiple JDBC frames (each frame being processed by a
   * potentially different thread), the thread that closes the yielder
   * (resulting in a QueryMetrics emit() call) may not be the same thread that
   * created the yielder (which initializes DefaultQueryMetrics with the current
   * thread as the owner). Create and close the yielder with this single-thread
   * executor to prevent this from happening.
   * <p>
   * The thread owner check in DefaultQueryMetrics is more aggressive than
   * needed for this specific JDBC case, since the JDBC frames are processed
   * sequentially. If the thread owner check is changed/loosened to permit this
   * use case, we would not need to use this executor.
   * <p>
   * See discussion at:
   * https://github.com/apache/druid/pull/4288
   * https://github.com/apache/druid/pull/4415
   */
  private final ExecutorService queryExecutor;
  private final DirectStatement stmt;
  private final long maxRowCount;
  private final ResultFetcherFactory fetcherFactory;
  private State state = State.NEW;
  private Meta.Signature signature;

  /**
   * The fetcher which reads batches of rows. Holds onto the yielder for a
   * query. Maintains the current read offset.
   */
  private ResultFetcher fetcher;

  /**
   * Future for a fetch that timed out waiting, and should be used again on
   * the next fetch request.
   */
  private Future<Meta.Frame> fetchFuture;

  /**
   * Cached version of the read offset in case the caller asks for the offset
   * concurrently with a fetch which may update its own offset. This offset
   * is that for the last batch that the client fetched: the fetcher itself
   * may be moving to a new offset.
   */
  private int nextFetchOffset;

  public DruidJdbcResultSet(
      final AbstractDruidJdbcStatement jdbcStatement,
      final DirectStatement stmt,
      final long maxRowCount,
      final ResultFetcherFactory fetcherFactory
  )
  {
    this.stmt = stmt;
    this.maxRowCount = maxRowCount;
    this.fetcherFactory = fetcherFactory;
    this.queryExecutor = Execs.singleThreaded(
        StringUtils.format(
            "JDBCQueryExecutor-connection-%s-statement-%d",
            StringUtils.encodeForFormat(jdbcStatement.getConnectionId()),
            jdbcStatement.getStatementId()
        )
    );
  }

  public synchronized void execute()
  {
    ensure(State.NEW);
    try {
      state = State.RUNNING;

      // Execute the first step: plan the query and return a sequence to use
      // to get values.
      final Sequence<Object[]> sequence = queryExecutor.submit(stmt::execute).get().getResults();

      // Subsequent fetch steps are done via the async "fetcher".
      fetcher = fetcherFactory.newFetcher(
          // We can't apply limits greater than Integer.MAX_VALUE, ignore them.
          maxRowCount >= 0 && maxRowCount <= Integer.MAX_VALUE ? (int) maxRowCount : Integer.MAX_VALUE,
          Yielders.each(sequence)
      );
      signature = AbstractDruidJdbcStatement.createSignature(
          stmt.prepareResult(),
          stmt.query().sql()
      );
      LOG.debug("Opened result set [%s]", stmt.sqlQueryId());
    }
    catch (ExecutionException e) {
      throw closeAndPropagateThrowable(e.getCause());
    }
    catch (Throwable t) {
      throw closeAndPropagateThrowable(t);
    }
  }

  public synchronized boolean isDone()
  {
    return state == State.DONE;
  }

  public synchronized Meta.Signature getSignature()
  {
    ensure(State.RUNNING, State.DONE);
    return signature;
  }

  public synchronized Meta.Frame nextFrame(final long fetchOffset, final int fetchMaxRowCount)
  {
    ensure(State.RUNNING, State.DONE);
    if (fetchOffset != nextFetchOffset) {
      throw new IAE(
          "Druid can only fetch forward. Requested offset of %,d != current offset %,d",
          fetchOffset,
          nextFetchOffset
      );
    }
    if (state == State.DONE) {
      LOG.debug("EOF at offset %,d for result set [%s]", fetchOffset, stmt.sqlQueryId());
      return new Meta.Frame(fetcher.offset(), true, Collections.emptyList());
    }

    final Future<Meta.Frame> future;
    if (fetchFuture == null) {
      // Not waiting on a batch. Request one now.
      fetcher.setBatchSize(fetchMaxRowCount);
      future = queryExecutor.submit(fetcher);
    } else {
      // Last batch took too long. Continue waiting for it.
      future = fetchFuture;
      fetchFuture = null;
    }
    try {
      Meta.Frame result = future.get(fetcherFactory.fetchTimeoutMs(), TimeUnit.MILLISECONDS);
      LOG.debug("Fetched batch at offset %,d for result set [%s]", fetchOffset, stmt.sqlQueryId());
      if (result.done) {
        state = State.DONE;
      }
      nextFetchOffset = fetcher.offset;
      return result;
    }
    catch (CancellationException | InterruptedException e) {
      // Consider this a failure.
      throw closeAndPropagateThrowable(e);
    }
    catch (ExecutionException e) {
      // Fetch threw an error. Unwrap it.
      throw closeAndPropagateThrowable(e.getCause());
    }
    catch (TimeoutException e) {
      LOG.debug("Timeout of batch at offset %,d for result set [%s]", fetchOffset, stmt.sqlQueryId());
      fetchFuture = future;
      // Wait timed out. Return 0 rows: the client will try again later.
      // We'll wait again on this same fetch next time.
      // Note that when the next fetch request comes, it will use the batch
      // size set here: any change in size will be ignored for the in-flight batch.
      // Changing batch size mid-query is an odd case: it will probably never happen.
      return new Meta.Frame(nextFetchOffset, false, Collections.emptyList());
    }
  }

  public synchronized long getCurrentOffset()
  {
    ensure(State.RUNNING, State.DONE);
    return fetcher.offset;
  }

  @GuardedBy("this")
  private void ensure(final State... desiredStates)
  {
    for (State desiredState : desiredStates) {
      if (state == desiredState) {
        return;
      }
    }
    throw new ISE("Invalid action for state [%s]", state);
  }

  private RuntimeException closeAndPropagateThrowable(Throwable t)
  {
    DruidMeta.logFailure(t);
    // Report a failure so that the failure is logged.
    stmt.reporter().failed(t);
    try {
      close();
    }
    catch (Throwable t1) {
      t.addSuppressed(t1);
    }
    finally {
      state = State.FAILED;
    }

    // Avoid unnecessary wrapping.
    if (t instanceof RuntimeException) {
      return (RuntimeException) t;
    }
    return new RuntimeException(t);
  }

  @Override
  public synchronized void close()
  {
    if (state == State.NEW) {
      state = State.CLOSED;
    }
    if (state == State.CLOSED || state == State.FAILED) {
      return;
    }
    LOG.debug("Closing result set [%s]", stmt.sqlQueryId());
    state = State.CLOSED;
    try {
      // If a fetch is in progress, wait for it to complete.
      if (fetchFuture != null) {
        try {
          fetchFuture.cancel(true);
          fetchFuture.get();
        }
        catch (Exception e) {
          // Ignore, we're shutting down anyway.
        }
        finally {
          fetchFuture = null;
        }
      }
      if (fetcher != null) {
        Yielder<Object[]> theYielder = fetcher.yielder;
        fetcher = null;

        // Put the close last, so any exceptions it throws are after we did the other cleanup above.
        queryExecutor.submit(
            () -> {
              theYielder.close();
              // makes this a Callable instead of Runnable so we don't need to catch exceptions inside the lambda
              return null;
            }
        ).get();

        queryExecutor.shutdownNow();
      }
    }
    catch (RuntimeException e) {
      throw e;
    }
    catch (Throwable t) {
      throw new RuntimeException(t);
    }
    finally {
      // Closing the statement cause logs and metrics to be emitted.
      stmt.close();
    }
  }

  private enum State
  {
    NEW,
    RUNNING,
    DONE,
    FAILED,
    CLOSED
  }
}
