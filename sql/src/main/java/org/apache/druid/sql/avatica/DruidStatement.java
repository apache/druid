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
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.calcite.rel.QueryMaker;

import java.io.Closeable;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Statement handle for {@link DruidMeta}. Thread-safe.
 */
public class DruidStatement implements Closeable
{
  public static final long START_OFFSET = 0;
  private final String connectionId;
  private final int statementId;
  private final Map<String, Object> queryContext;
  private final SqlLifecycle sqlLifecycle;
  private final Runnable onClose;
  private final Object lock = new Object();
  /**
   * Query metrics can only be used within a single thread. Because results can be paginated into multiple
   * JDBC frames (each frame being processed by a potentially different thread), the thread that closes the yielder
   * (resulting in a QueryMetrics emit() call) may not be the same thread that created the yielder (which initializes
   * DefaultQueryMetrics with the current thread as the owner). Create and close the yielder with this
   * single-thread executor to prevent this from happening.
   * <p>
   * The thread owner check in DefaultQueryMetrics is more aggressive than needed for this specific JDBC case, since
   * the JDBC frames are processed sequentially. If the thread owner check is changed/loosened to permit this use case,
   * we would not need to use this executor.
   * <p>
   * See discussion at:
   * https://github.com/apache/incubator-druid/pull/4288
   * https://github.com/apache/incubator-druid/pull/4415
   */
  private final ExecutorService yielderOpenCloseExecutor;
  private State state = State.NEW;
  private String query;
  private long maxRowCount;
  private Meta.Signature signature;
  private Yielder<Object[]> yielder;
  private int offset = 0;
  private Throwable throwable;

  public DruidStatement(
      final String connectionId,
      final int statementId,
      final Map<String, Object> queryContext,
      final SqlLifecycle sqlLifecycle,
      final Runnable onClose
  )
  {
    this.connectionId = Preconditions.checkNotNull(connectionId, "connectionId");
    this.statementId = statementId;
    this.queryContext = queryContext == null ? ImmutableMap.of() : queryContext;
    this.sqlLifecycle = Preconditions.checkNotNull(sqlLifecycle, "sqlLifecycle");
    this.onClose = Preconditions.checkNotNull(onClose, "onClose");
    this.yielderOpenCloseExecutor = Execs.singleThreaded(
        StringUtils.format("JDBCYielderOpenCloseExecutor-connection-%s-statement-%d", connectionId, statementId)
    );
  }

  public static List<ColumnMetaData> createColumnMetaData(final RelDataType rowType)
  {
    final List<ColumnMetaData> columns = new ArrayList<>();
    List<RelDataTypeField> fieldList = rowType.getFieldList();

    for (int i = 0; i < fieldList.size(); i++) {
      RelDataTypeField field = fieldList.get(i);
      final ColumnMetaData.Rep rep = QueryMaker.rep(field.getType().getSqlTypeName());
      final ColumnMetaData.ScalarType columnType = ColumnMetaData.scalar(
          field.getType().getSqlTypeName().getJdbcOrdinal(),
          field.getType().getSqlTypeName().getName(),
          rep
      );
      columns.add(
          new ColumnMetaData(
              i, // ordinal
              false, // auto increment
              true, // case sensitive
              false, // searchable
              false, // currency
              field.getType().isNullable()
              ? DatabaseMetaData.columnNullable
              : DatabaseMetaData.columnNoNulls, // nullable
              true, // signed
              field.getType().getPrecision(), // display size
              field.getName(), // label
              null, // column name
              null, // schema name
              field.getType().getPrecision(), // precision
              field.getType().getScale(), // scale
              null, // table name
              null, // catalog name
              columnType, // avatica type
              true, // read only
              false, // writable
              false, // definitely writable
              columnType.columnClassName() // column class name
          )
      );
    }

    return columns;
  }

  public DruidStatement prepare(
      final String query,
      final long maxRowCount,
      final AuthenticationResult authenticationResult
  )
  {
    synchronized (lock) {
      try {
        ensure(State.NEW);
        sqlLifecycle.initialize(query, queryContext);
        sqlLifecycle.planAndAuthorize(authenticationResult);
        this.maxRowCount = maxRowCount;
        this.query = query;
        this.signature = Meta.Signature.create(
            createColumnMetaData(sqlLifecycle.rowType()),
            query,
            new ArrayList<>(),
            Meta.CursorFactory.ARRAY,
            Meta.StatementType.SELECT // We only support SELECT
        );
        this.state = State.PREPARED;
      }
      catch (Throwable t) {
        this.throwable = t;
        try {
          close();
        }
        catch (Throwable t1) {
          t.addSuppressed(t1);
        }
        throw new RuntimeException(t);
      }

      return this;
    }
  }

  public DruidStatement execute()
  {
    synchronized (lock) {
      ensure(State.PREPARED);

      try {
        final Sequence<Object[]> baseSequence = yielderOpenCloseExecutor.submit(
            sqlLifecycle::execute
        ).get();

        // We can't apply limits greater than Integer.MAX_VALUE, ignore them.
        final Sequence<Object[]> retSequence =
            maxRowCount >= 0 && maxRowCount <= Integer.MAX_VALUE
            ? baseSequence.limit((int) maxRowCount)
            : baseSequence;

        yielder = Yielders.each(retSequence);
        state = State.RUNNING;
      }
      catch (Throwable t) {
        this.throwable = t;
        try {
          close();
        }
        catch (Throwable t1) {
          t.addSuppressed(t1);
        }
        throw new RuntimeException(t);
      }

      return this;
    }
  }

  public String getConnectionId()
  {
    return connectionId;
  }

  public int getStatementId()
  {
    return statementId;
  }

  public String getQuery()
  {
    synchronized (lock) {
      ensure(State.PREPARED, State.RUNNING, State.DONE);
      return query;
    }
  }

  public Meta.Signature getSignature()
  {
    synchronized (lock) {
      ensure(State.PREPARED, State.RUNNING, State.DONE);
      return signature;
    }
  }

  public RelDataType getRowType()
  {
    synchronized (lock) {
      ensure(State.PREPARED, State.RUNNING, State.DONE);
      return sqlLifecycle.rowType();
    }
  }

  public long getCurrentOffset()
  {
    synchronized (lock) {
      ensure(State.RUNNING, State.DONE);
      return offset;
    }
  }

  public boolean isDone()
  {
    synchronized (lock) {
      return state == State.DONE;
    }
  }

  public Meta.Frame nextFrame(final long fetchOffset, final int fetchMaxRowCount)
  {
    synchronized (lock) {
      ensure(State.RUNNING);
      Preconditions.checkState(fetchOffset == offset, "fetchOffset[%,d] != offset[%,d]", fetchOffset, offset);

      try {
        final List<Object> rows = new ArrayList<>();
        while (!yielder.isDone() && (fetchMaxRowCount < 0 || offset < fetchOffset + fetchMaxRowCount)) {
          rows.add(yielder.get());
          yielder = yielder.next(null);
          offset++;
        }

        final boolean done = yielder.isDone();
        if (done) {
          close();
        }

        return new Meta.Frame(fetchOffset, done, rows);
      }
      catch (Throwable t) {
        this.throwable = t;
        try {
          close();
        }
        catch (Throwable t1) {
          t.addSuppressed(t1);
        }
        throw t;
      }
    }
  }

  @Override
  public void close()
  {
    State oldState = null;
    try {
      synchronized (lock) {
        oldState = state;
        state = State.DONE;
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
    }
    catch (Throwable t) {
      if (oldState != State.DONE) {
        // First close. Run the onClose function.
        try {
          onClose.run();
          sqlLifecycle.emitLogsAndMetrics(t, null, -1);
        }
        catch (Throwable t1) {
          t.addSuppressed(t1);
        }
      }

      throw new RuntimeException(t);
    }

    if (oldState != State.DONE) {
      // First close. Run the onClose function.
      try {
        if (!(this.throwable instanceof ForbiddenException)) {
          sqlLifecycle.emitLogsAndMetrics(this.throwable, null, -1);
        }
        onClose.run();
      }
      catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }
  }

  @GuardedBy("lock")
  private void ensure(final State... desiredStates)
  {
    for (State desiredState : desiredStates) {
      if (state == desiredState) {
        return;
      }
    }
    throw new ISE("Invalid action for state[%s]", state);
  }

  enum State
  {
    NEW,
    PREPARED,
    RUNNING,
    DONE
  }
}
