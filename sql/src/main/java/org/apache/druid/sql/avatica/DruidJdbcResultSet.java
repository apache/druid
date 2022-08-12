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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.sql.DirectStatement;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

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
 */
public class DruidJdbcResultSet implements Closeable
{
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
  private final ExecutorService yielderOpenCloseExecutor;
  private final DirectStatement stmt;
  private final long maxRowCount;
  private State state = State.NEW;
  private Meta.Signature signature;
  private Yielder<Object[]> yielder;
  private int offset;

  public DruidJdbcResultSet(
      final AbstractDruidJdbcStatement jdbcStatement,
      DirectStatement stmt,
      final long maxRowCount
  )
  {
    this.stmt = stmt;
    this.maxRowCount = maxRowCount;
    this.yielderOpenCloseExecutor = Execs.singleThreaded(
        StringUtils.format(
            "JDBCYielderOpenCloseExecutor-connection-%s-statement-%d",
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
      final Sequence<Object[]> baseSequence = yielderOpenCloseExecutor.submit(stmt::execute).get();

      // We can't apply limits greater than Integer.MAX_VALUE, ignore them.
      final Sequence<Object[]> retSequence =
          maxRowCount >= 0 && maxRowCount <= Integer.MAX_VALUE
          ? baseSequence.limit((int) maxRowCount)
          : baseSequence;

      yielder = Yielders.each(retSequence);
      signature = AbstractDruidJdbcStatement.createSignature(
          stmt.prepareResult(),
          stmt.sqlRequest().sql()
      );
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
    Preconditions.checkState(fetchOffset == offset, "fetchOffset [%,d] != offset [%,d]", fetchOffset, offset);
    if (state == State.DONE) {
      return new Meta.Frame(fetchOffset, true, Collections.emptyList());
    }

    try {
      final List<Object> rows = new ArrayList<>();
      while (!yielder.isDone() && (fetchMaxRowCount < 0 || offset < fetchOffset + fetchMaxRowCount)) {
        rows.add(yielder.get());
        yielder = yielder.next(null);
        offset++;
      }

      if (yielder.isDone()) {
        state = State.DONE;
      }

      return new Meta.Frame(fetchOffset, state == State.DONE, rows);
    }
    catch (Throwable t) {
      throw closeAndPropagateThrowable(t);
    }
  }

  public synchronized long getCurrentOffset()
  {
    ensure(State.RUNNING, State.DONE);
    return offset;
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
    state = State.CLOSED;
    try {
      if (yielder != null) {
        Yielder<Object[]> theYielder = this.yielder;
        this.yielder = null;

        // Put the close last, so any exceptions it throws are after we did the other cleanup above.
        yielderOpenCloseExecutor.submit(
            () -> {
              theYielder.close();
              // makes this a Callable instead of Runnable so we don't need to catch exceptions inside the lambda
              return null;
            }
        ).get();

        yielderOpenCloseExecutor.shutdownNow();
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
