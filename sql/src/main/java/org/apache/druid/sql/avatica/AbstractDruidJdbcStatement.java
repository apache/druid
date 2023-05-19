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
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.sql.avatica.DruidJdbcResultSet.ResultFetcherFactory;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PrepareResult;

import java.io.Closeable;
import java.sql.Array;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;

/**
 * Common implementation for the JDBC {@code Statement} and
 * {@code PreparedStatement} implementations in Druid. Statement use
 * {@link DruidJdbcResultSet} objects to iterate through rows: zero
 * or one may be open at any time, and a single statement supports
 * multiple result sets concurrently. Druid closes the result set after
 * the last batch in compliance with this note on page 137 of the
 * <a href="https://download.oracle.com/otn-pub/jcp/jdbc-4_1-mrel-spec/jdbc4.1-fr-spec.pdf?AuthParam=1655858260_d6388aeec9aeb8e5616d221749b1ff71">
 * JDBC 4.1 specification</a>:
 * <p>
 * <i>Some JDBC driver implementations may also implicitly close the
 * ResultSet when the ResultSet type is TYPE_FORWARD_ONLY and the next
 * method of ResultSet returns false.</i>
 */
public abstract class AbstractDruidJdbcStatement implements Closeable
{
  public static final long START_OFFSET = 0;

  protected final String connectionId;
  protected final int statementId;
  protected final ResultFetcherFactory fetcherFactory;
  protected Throwable throwable;
  protected DruidJdbcResultSet resultSet;

  public AbstractDruidJdbcStatement(
      final String connectionId,
      final int statementId,
      final ResultFetcherFactory fetcherFactory
  )
  {
    this.connectionId = Preconditions.checkNotNull(connectionId, "connectionId");
    this.statementId = statementId;
    this.fetcherFactory = fetcherFactory;
  }

  protected static Meta.Signature createSignature(
      final PrepareResult prepareResult,
      final String sql
  )
  {
    List<AvaticaParameter> params = new ArrayList<>();
    final RelDataType parameterRowType = prepareResult.getParameterRowType();
    for (RelDataTypeField field : parameterRowType.getFieldList()) {
      RelDataType type = field.getType();
      params.add(createParameter(field, type));
    }
    return Meta.Signature.create(
        createColumnMetaData(prepareResult.getReturnedRowType()),
        sql,
        params,
        Meta.CursorFactory.ARRAY,
        Meta.StatementType.SELECT // We only support SELECT
    );
  }

  private static AvaticaParameter createParameter(
      final RelDataTypeField field,
      final RelDataType type
  )
  {
    // signed is always false because no way to extract from RelDataType, and the only usage of this AvaticaParameter
    // constructor I can find, in CalcitePrepareImpl, does it this way with hard coded false
    return new AvaticaParameter(
        false,
        type.getPrecision(),
        type.getScale(),
        type.getSqlTypeName().getJdbcOrdinal(),
        type.getSqlTypeName().getName(),
        Calcites.sqlTypeNameJdbcToJavaClass(type.getSqlTypeName()).getName(),
        field.getName()
    );
  }

  public static List<ColumnMetaData> createColumnMetaData(final RelDataType rowType)
  {
    final List<ColumnMetaData> columns = new ArrayList<>();
    List<RelDataTypeField> fieldList = rowType.getFieldList();

    for (int i = 0; i < fieldList.size(); i++) {
      RelDataTypeField field = fieldList.get(i);

      final ColumnMetaData.AvaticaType columnType;
      if (field.getType().getSqlTypeName() == SqlTypeName.ARRAY) {
        final ColumnMetaData.Rep elementRep = rep(field.getType().getComponentType().getSqlTypeName());
        final ColumnMetaData.ScalarType elementType = ColumnMetaData.scalar(
            field.getType().getComponentType().getSqlTypeName().getJdbcOrdinal(),
            field.getType().getComponentType().getSqlTypeName().getName(),
            elementRep
        );
        final ColumnMetaData.Rep arrayRep = rep(field.getType().getSqlTypeName());
        columnType = ColumnMetaData.array(
            elementType,
            field.getType().getSqlTypeName().getName(),
            arrayRep
        );
      } else {
        final ColumnMetaData.Rep rep = rep(field.getType().getSqlTypeName());
        columnType = ColumnMetaData.scalar(
            field.getType().getSqlTypeName().getJdbcOrdinal(),
            field.getType().getSqlTypeName().getName(),
            rep
        );
      }
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

  private static ColumnMetaData.Rep rep(final SqlTypeName sqlType)
  {
    if (SqlTypeName.CHAR_TYPES.contains(sqlType)) {
      return ColumnMetaData.Rep.of(String.class);
    } else if (sqlType == SqlTypeName.TIMESTAMP) {
      return ColumnMetaData.Rep.of(Long.class);
    } else if (sqlType == SqlTypeName.DATE) {
      return ColumnMetaData.Rep.of(Integer.class);
    } else if (sqlType == SqlTypeName.INTEGER) {
      // use Number.class for exact numeric types since JSON transport might switch longs to integers
      return ColumnMetaData.Rep.of(Number.class);
    } else if (sqlType == SqlTypeName.BIGINT) {
      // use Number.class for exact numeric types since JSON transport might switch longs to integers
      return ColumnMetaData.Rep.of(Number.class);
    } else if (sqlType == SqlTypeName.FLOAT) {
      return ColumnMetaData.Rep.of(Float.class);
    } else if (sqlType == SqlTypeName.DOUBLE || sqlType == SqlTypeName.DECIMAL) {
      return ColumnMetaData.Rep.of(Double.class);
    } else if (sqlType == SqlTypeName.BOOLEAN) {
      return ColumnMetaData.Rep.of(Boolean.class);
    } else if (sqlType == SqlTypeName.OTHER) {
      return ColumnMetaData.Rep.of(Object.class);
    } else if (sqlType == SqlTypeName.ARRAY) {
      return ColumnMetaData.Rep.of(Array.class);
    } else {
      throw new ISE("No rep for SQL type [%s]", sqlType);
    }
  }

  public Meta.Frame nextFrame(final long fetchOffset, final int fetchMaxRowCount)
  {
    Meta.Frame frame = requireResultSet().nextFrame(fetchOffset, fetchMaxRowCount);

    // Implicitly close after the last result frame.
    if (frame.done) {
      closeResultSet();
    }
    return frame;
  }

  public abstract Meta.Signature getSignature();

  public void closeResultSet()
  {
    // Lock held only to get the result set, not during cleanup.
    DruidJdbcResultSet currentResultSet;
    synchronized (this) {
      currentResultSet = resultSet;
      resultSet = null;
    }
    if (currentResultSet != null) {
      currentResultSet.close();
    }
  }

  protected synchronized DruidJdbcResultSet requireResultSet()
  {
    if (resultSet == null) {
      throw new ISE("No result set open for statement [%d]", statementId);
    }
    return resultSet;
  }

  public long getCurrentOffset()
  {
    return requireResultSet().getCurrentOffset();
  }

  public synchronized boolean isDone()
  {
    return resultSet == null ? true : resultSet.isDone();
  }

  @Override
  public synchronized void close()
  {
    closeResultSet();
  }

  public String getConnectionId()
  {
    return connectionId;
  }

  public int getStatementId()
  {
    return statementId;
  }
}
