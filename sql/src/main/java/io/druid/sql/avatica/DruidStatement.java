/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.avatica;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.Yielders;
import io.druid.sql.calcite.planner.DruidPlanner;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.planner.PlannerResult;
import io.druid.sql.calcite.rel.QueryMaker;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Statement handle for {@link DruidMeta}. Thread-safe.
 */
public class DruidStatement implements Closeable
{
  public static final long START_OFFSET = 0;

  enum State
  {
    NEW,
    PREPARED,
    RUNNING,
    DONE
  }

  private final String connectionId;
  private final int statementId;
  private final Map<String, Object> queryContext;
  private final Object lock = new Object();

  private State state = State.NEW;
  private String query;
  private long maxRowCount;
  private PlannerResult plannerResult;
  private Meta.Signature signature;
  private Yielder<Object[]> yielder;
  private int offset = 0;

  public DruidStatement(final String connectionId, final int statementId, final Map<String, Object> queryContext)
  {
    this.connectionId = connectionId;
    this.statementId = statementId;
    this.queryContext = queryContext;
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

  public DruidStatement prepare(final PlannerFactory plannerFactory, final String query, final long maxRowCount)
  {
    try (final DruidPlanner planner = plannerFactory.createPlanner(queryContext)) {
      synchronized (lock) {
        ensure(State.NEW);
        this.plannerResult = planner.plan(query);
        this.maxRowCount = maxRowCount;
        this.query = query;
        this.signature = Meta.Signature.create(
            createColumnMetaData(plannerResult.rowType()),
            query,
            new ArrayList<AvaticaParameter>(),
            Meta.CursorFactory.ARRAY,
            Meta.StatementType.SELECT // We only support SELECT
        );
        this.state = State.PREPARED;
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    return this;
  }

  public DruidStatement execute()
  {
    synchronized (lock) {
      ensure(State.PREPARED);

      final Sequence<Object[]> baseSequence = plannerResult.run();

      // We can't apply limits greater than Integer.MAX_VALUE, ignore them.
      final Sequence<Object[]> retSequence =
          maxRowCount >= 0 && maxRowCount <= Integer.MAX_VALUE
          ? Sequences.limit(baseSequence, (int) maxRowCount)
          : baseSequence;

      yielder = Yielders.each(retSequence);
      state = State.RUNNING;

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
      return plannerResult.rowType();
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
    synchronized (lock) {
      state = State.DONE;

      if (yielder != null) {
        Yielder<Object[]> theYielder = this.yielder;
        this.yielder = null;

        // Put the close last, so any exceptions it throws are after we did the other cleanup above.
        try {
          theYielder.close();
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
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
}
