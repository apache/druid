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

package org.apache.druid.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jdbc.http.ColumnMetadata;
import org.apache.druid.jdbc.http.EmptyQueryResultsIterator;
import org.apache.druid.jdbc.http.QueryResultsIterator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Base64;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Our implementation of JDBC {@link ResultSet}. Forward-only and read-only. Must be closed in order to
 * release the underlying HTTP response.
 *
 * <p><b>Thread safety:</b> {@link #close()} and {@link #isClosed()} are safe to call from any
 * thread. All other methods are not.
 */
public class DruidResultSet implements ResultSet
{
  private final QueryResultsIterator resultsIterator;

  @Nullable
  private final DruidStatement statement;

  /**
   * Used by {@link #coerceToString(Object)} to write list-valued columns as JSON.
   */
  @Nullable
  private final ObjectMapper jsonMapper;

  /**
   * Whether this resultset is closed. Set by {@link #close()}.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Lowercased column name to 1-based column index, for {@link #findColumn(String)}. Built on first use, since
   * many callers only ever access columns by index.
   */
  @Nullable
  private Map<String, Integer> columnIndexByLowercaseName;

  /**
   * Zero-based index of the current row; -1 before the first call to {@link #next()}.
   */
  private int currentRowIndex = -1;

  /**
   * Values of the current row, or null when positioned before the first row or after the last row.
   */
  @Nullable
  private Object[] currentRow;

  /**
   * Whether the most recently read column value was null. Set by {@link #getCurrentRowValue(int)} and returned
   * by {@link #wasNull()}.
   */
  private boolean lastValueWasNull;

  public DruidResultSet(
      final QueryResultsIterator resultsIterator,
      @Nullable final DruidStatement statement,
      @Nullable final ObjectMapper jsonMapper
  )
  {
    this.resultsIterator = Objects.requireNonNull(resultsIterator, "resultsIterator");
    this.statement = statement;
    this.jsonMapper = jsonMapper;
  }

  /**
   * Create an empty resultset with the provided columns.
   */
  public static ResultSet createEmpty(final List<ColumnMetadata> columns)
  {
    return new DruidResultSet(
        new EmptyQueryResultsIterator(columns),
        null, // No DruidStatement to close, null is OK
        null // No ObjectMapper needed for empty results
    );
  }

  @Override
  public boolean next() throws SQLException
  {
    throwIfClosed();

    if (resultsIterator.hasNext()) {
      currentRow = resultsIterator.next();
      currentRowIndex++;
      return true;
    } else {
      currentRow = null;
      return false;
    }
  }

  @Override
  public void close() throws SQLException
  {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    try {
      resultsIterator.close();
    }
    catch (IOException e) {
      throw new DruidJdbcException(e, "Failed to close results iterator");
    }
    finally {
      if (statement != null) {
        statement.onResultSetClosed(this);
      }
    }
  }

  @Override
  public boolean wasNull() throws SQLException
  {
    throwIfClosed();
    return lastValueWasNull;
  }

  @Override
  @Nullable
  public String getString(final int columnIndex) throws SQLException
  {
    final Object value = getCurrentRowValue(columnIndex);
    if (value == null) {
      return null;
    }
    return coerceToString(value);
  }

  @Override
  public boolean getBoolean(final int columnIndex) throws SQLException
  {
    final Object value = getCurrentRowValue(columnIndex);
    if (value == null) {
      return false;
    }
    return toBoolean(value, columnIndex);
  }

  @Override
  public byte getByte(final int columnIndex) throws SQLException
  {
    final Number value = getNumericValue(columnIndex, "byte");
    return value == null ? 0 : (byte) value.longValue();
  }

  @Override
  public short getShort(final int columnIndex) throws SQLException
  {
    final Number value = getNumericValue(columnIndex, "short");
    return value == null ? 0 : (short) value.longValue();
  }

  @Override
  public int getInt(final int columnIndex) throws SQLException
  {
    final Number value = getNumericValue(columnIndex, "int");
    return value == null ? 0 : (int) value.longValue();
  }

  @Override
  public long getLong(final int columnIndex) throws SQLException
  {
    final Number value = getNumericValue(columnIndex, "long");
    return value == null ? 0 : value.longValue();
  }

  @Override
  public float getFloat(final int columnIndex) throws SQLException
  {
    final Number value = getNumericValue(columnIndex, "float");
    return value == null ? 0 : (float) value.doubleValue();
  }

  @Override
  public double getDouble(final int columnIndex) throws SQLException
  {
    final Number value = getNumericValue(columnIndex, "double");
    return value == null ? 0 : value.doubleValue();
  }

  @Override
  @Nullable
  public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException
  {
    final BigDecimal value = getBigDecimal(columnIndex);
    if (value == null) {
      return null;
    }
    return value.setScale(scale, RoundingMode.HALF_UP);
  }

  @Override
  @Nullable
  public byte[] getBytes(final int columnIndex) throws SQLException
  {
    final Object value = getCurrentRowValue(columnIndex);
    if (value == null) {
      return null;
    }
    return toBytes(value, columnIndex);
  }

  @Override
  @Nullable
  public Date getDate(final int columnIndex) throws SQLException
  {
    return getDate(columnIndex, null);
  }

  @Override
  @Nullable
  public Time getTime(final int columnIndex) throws SQLException
  {
    return getTime(columnIndex, null);
  }

  @Override
  @Nullable
  public Timestamp getTimestamp(final int columnIndex) throws SQLException
  {
    return getTimestamp(columnIndex, null);
  }

  @Override
  public InputStream getAsciiStream(final int columnIndex) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getAsciiStream not supported");
  }

  @Override
  public InputStream getUnicodeStream(final int columnIndex) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getUnicodeStream not supported");
  }

  @Override
  public InputStream getBinaryStream(final int columnIndex) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getBinaryStream not supported");
  }

  @Override
  public String getString(final String columnLabel) throws SQLException
  {
    return getString(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(final String columnLabel) throws SQLException
  {
    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(final String columnLabel) throws SQLException
  {
    return getByte(findColumn(columnLabel));
  }

  @Override
  public short getShort(final String columnLabel) throws SQLException
  {
    return getShort(findColumn(columnLabel));
  }

  @Override
  public int getInt(final String columnLabel) throws SQLException
  {
    return getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(final String columnLabel) throws SQLException
  {
    return getLong(findColumn(columnLabel));
  }

  @Override
  public float getFloat(final String columnLabel) throws SQLException
  {
    return getFloat(findColumn(columnLabel));
  }

  @Override
  public double getDouble(final String columnLabel) throws SQLException
  {
    return getDouble(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException
  {
    return getBigDecimal(findColumn(columnLabel), scale);
  }

  @Override
  public byte[] getBytes(final String columnLabel) throws SQLException
  {
    return getBytes(findColumn(columnLabel));
  }

  @Override
  @Nullable
  public Date getDate(final String columnLabel) throws SQLException
  {
    return getDate(findColumn(columnLabel));
  }

  @Override
  @Nullable
  public Time getTime(final String columnLabel) throws SQLException
  {
    return getTime(findColumn(columnLabel));
  }

  @Override
  @Nullable
  public Timestamp getTimestamp(final String columnLabel) throws SQLException
  {
    return getTimestamp(findColumn(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(final String columnLabel) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getAsciiStream not supported");
  }

  @Override
  public InputStream getUnicodeStream(final String columnLabel) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getUnicodeStream not supported");
  }

  @Override
  public InputStream getBinaryStream(final String columnLabel) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getBinaryStream not supported");
  }

  @Override
  @Nullable
  public SQLWarning getWarnings() throws SQLException
  {
    throwIfClosed();
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException
  {
    throwIfClosed();
  }

  @Override
  public String getCursorName() throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("Cursors not supported");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException
  {
    throwIfClosed();
    return new DruidResultSetMetaData(resultsIterator.getColumns());
  }

  @Override
  public Object getObject(final int columnIndex) throws SQLException
  {
    return coerceForGetObject(getCurrentRowValue(columnIndex), columnIndex);
  }

  @Override
  public Object getObject(final String columnLabel) throws SQLException
  {
    return getObject(findColumn(columnLabel));
  }

  @Override
  public int findColumn(final String columnLabel) throws SQLException
  {
    throwIfClosed();

    if (columnIndexByLowercaseName == null) {
      // Per the java.sql.ResultSet javadoc: "Column names used as input to getter methods are case insensitive.
      // When a getter method is called with a column name and several columns have the same name, the value of
      // the first matching column will be returned." Hence putIfAbsent.
      final List<ColumnMetadata> columns = resultsIterator.getColumns();
      final Map<String, Integer> map = new HashMap<>();
      for (int i = 0; i < columns.size(); i++) {
        map.putIfAbsent(columns.get(i).name().toLowerCase(Locale.ENGLISH), i + 1); // JDBC columns are 1-based
      }
      columnIndexByLowercaseName = map;
    }

    final Integer columnIndex = columnIndexByLowercaseName.get(columnLabel.toLowerCase(Locale.ENGLISH));
    if (columnIndex == null) {
      throw new DruidJdbcException("Column not found: %s", columnLabel);
    }
    return columnIndex;
  }

  @Override
  public Reader getCharacterStream(final int columnIndex) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getCharacterStream not supported");
  }

  @Override
  public Reader getCharacterStream(final String columnLabel) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getCharacterStream not supported");
  }

  @Override
  @Nullable
  public BigDecimal getBigDecimal(final int columnIndex) throws SQLException
  {
    final Object value = getCurrentRowValue(columnIndex);
    if (value == null) {
      return null;
    }
    return toBigDecimal(value, columnIndex);
  }

  @Override
  public BigDecimal getBigDecimal(final String columnLabel) throws SQLException
  {
    return getBigDecimal(findColumn(columnLabel));
  }

  @Override
  public boolean isBeforeFirst() throws SQLException
  {
    throwIfClosed();
    return currentRowIndex < 0 && resultsIterator.hasNext();
  }

  @Override
  public boolean isAfterLast() throws SQLException
  {
    throwIfClosed();
    // True only once next() has returned false, which is when it clears currentRow.
    return currentRowIndex >= 0 && currentRow == null;
  }

  @Override
  public boolean isFirst() throws SQLException
  {
    throwIfClosed();
    return currentRow != null && currentRowIndex == 0;
  }

  @Override
  public boolean isLast() throws SQLException
  {
    throwIfClosed();
    // Requires a lookahead, which for a streaming iterator may block until the next row arrives. It does not
    // consume the row: hasNext() is idempotent.
    return currentRow != null && !resultsIterator.hasNext();
  }

  @Override
  public void beforeFirst() throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("beforeFirst not supported");
  }

  @Override
  public void afterLast() throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("afterLast not supported");
  }

  @Override
  public boolean first() throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("first not supported");
  }

  @Override
  public boolean last() throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("last not supported");
  }

  @Override
  public int getRow() throws SQLException
  {
    throwIfClosed();
    if (currentRow == null) {
      return 0;
    } else {
      return currentRowIndex + 1; // JDBC rows are 1-based
    }
  }

  @Override
  public boolean absolute(final int row) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("absolute not supported");
  }

  @Override
  public boolean relative(final int rows) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("relative not supported");
  }

  @Override
  public boolean previous() throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("previous not supported");
  }

  @Override
  public int getFetchDirection() throws SQLException
  {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchDirection(final int direction) throws SQLException
  {
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new DruidJdbcFeatureNotSupportedException("Only FETCH_FORWARD is supported");
    }
  }

  @Override
  public int getFetchSize() throws SQLException
  {
    return 0;
  }

  @Override
  public void setFetchSize(final int rows) throws SQLException
  {
    // Ignore fetch size
  }

  @Override
  public int getType() throws SQLException
  {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() throws SQLException
  {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public boolean rowUpdated() throws SQLException
  {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException
  {
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException
  {
    return false;
  }

  // Update methods - all throw exceptions since ResultSet is read-only
  @Override
  public void updateNull(final int columnIndex) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBoolean(final int columnIndex, final boolean x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateByte(final int columnIndex, final byte x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateShort(final int columnIndex, final short x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateInt(final int columnIndex, final int x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateLong(final int columnIndex, final long x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateFloat(final int columnIndex, final float x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateDouble(final int columnIndex, final double x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBigDecimal(final int columnIndex, final BigDecimal x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateString(final int columnIndex, final String x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBytes(final int columnIndex, final byte[] x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateDate(final int columnIndex, final Date x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateTime(final int columnIndex, final Time x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateTimestamp(final int columnIndex, final Timestamp x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateAsciiStream(final int columnIndex, final InputStream x, final int length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBinaryStream(final int columnIndex, final InputStream x, final int length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateCharacterStream(final int columnIndex, final Reader x, final int length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateObject(final int columnIndex, final Object x, final int scaleOrLength) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateObject(final int columnIndex, final Object x) throws SQLException
  {
    throw updatesNotSupported();
  }

  // String-based update methods
  @Override
  public void updateNull(final String columnLabel) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBoolean(final String columnLabel, final boolean x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateByte(final String columnLabel, final byte x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateShort(final String columnLabel, final short x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateInt(final String columnLabel, final int x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateLong(final String columnLabel, final long x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateFloat(final String columnLabel, final float x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateDouble(final String columnLabel, final double x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBigDecimal(final String columnLabel, final BigDecimal x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateString(final String columnLabel, final String x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBytes(final String columnLabel, final byte[] x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateDate(final String columnLabel, final Date x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateTime(final String columnLabel, final Time x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateTimestamp(final String columnLabel, final Timestamp x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateAsciiStream(final String columnLabel, final InputStream x, final int length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBinaryStream(final String columnLabel, final InputStream x, final int length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateCharacterStream(final String columnLabel, final Reader reader, final int length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateObject(final String columnLabel, final Object x, final int scaleOrLength) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateObject(final String columnLabel, final Object x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void insertRow() throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateRow() throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void deleteRow() throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void refreshRow() throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("refreshRow not supported");
  }

  @Override
  public void cancelRowUpdates() throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void moveToInsertRow() throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void moveToCurrentRow() throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  @Nullable
  public Statement getStatement() throws SQLException
  {
    throwIfClosed();
    return statement;
  }

  @Override
  public Object getObject(final int columnIndex, final Map<String, Class<?>> map) throws SQLException
  {
    return getObject(columnIndex);
  }

  @Override
  public Ref getRef(final int columnIndex) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getRef not supported");
  }

  @Override
  public Blob getBlob(final int columnIndex) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getBlob not supported");
  }

  @Override
  public Clob getClob(final int columnIndex) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getClob not supported");
  }

  @Override
  @Nullable
  public Array getArray(final int columnIndex) throws SQLException
  {
    final Object value = getCurrentRowValue(columnIndex);
    if (value == null) {
      return null;
    }
    return coerceToArray(value, columnIndex);
  }

  @Override
  public Object getObject(final String columnLabel, final Map<String, Class<?>> map) throws SQLException
  {
    return getObject(columnLabel);
  }

  @Override
  public Ref getRef(final String columnLabel) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getRef not supported");
  }

  @Override
  public Blob getBlob(final String columnLabel) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getBlob not supported");
  }

  @Override
  public Clob getClob(final String columnLabel) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getClob not supported");
  }

  @Override
  public Array getArray(final String columnLabel) throws SQLException
  {
    return getArray(findColumn(columnLabel));
  }

  @Override
  @Nullable
  public Date getDate(final int columnIndex, @Nullable final Calendar cal) throws SQLException
  {
    final Object value = getCurrentRowValue(columnIndex);
    if (value == null) {
      return null;
    }

    if (value instanceof Date date) {
      // A DATE column: re-anchor the stored day, rather than truncating an instant.
      if (cal == null) {
        return date;
      } else {
        return new Date(date.toLocalDate().atStartOfDay(zoneOf(cal)).toInstant().toEpochMilli());
      }
    } else {
      return toDate(toEpochMillis(value, columnIndex, "Date"), zoneOf(cal));
    }
  }

  @Override
  @Nullable
  public Date getDate(final String columnLabel, @Nullable final Calendar cal) throws SQLException
  {
    return getDate(findColumn(columnLabel), cal);
  }

  /**
   * {@inheritDoc}
   *
   * <p>As in {@link #getDate(int, Calendar)}, {@code cal} does not affect which instant the value denotes,
   * but does select the time zone whose reading of that instant's time of day is returned. The returned
   * value places that time of day on 1970-01-01, as {@link Time} requires. This is the inverse of
   * {@link DruidPreparedStatement#setTime(int, Time, Calendar)}, which reads the time of day off a
   * {@link Time} in {@code cal}'s time zone.
   */
  @Override
  @Nullable
  public Time getTime(final int columnIndex, @Nullable final Calendar cal) throws SQLException
  {
    final Object value = getCurrentRowValue(columnIndex);
    if (value == null) {
      return null;
    }
    return toTime(toEpochMillis(value, columnIndex, "Time"), zoneOf(cal));
  }

  @Override
  @Nullable
  public Time getTime(final String columnLabel, @Nullable final Calendar cal) throws SQLException
  {
    return getTime(findColumn(columnLabel), cal);
  }

  /**
   * {@inheritDoc}
   *
   * <p>The {@code cal} argument is ignored: Druid's JSON results carry an explicit UTC offset on every temporal
   * value, so the value already denotes a single instant and there is nothing for {@code cal} to resolve.
   */
  @Override
  @Nullable
  public Timestamp getTimestamp(final int columnIndex, @Nullable final Calendar cal) throws SQLException
  {
    final Object value = getCurrentRowValue(columnIndex);
    if (value == null) {
      return null;
    }

    // Return an existing Timestamp as-is to preserve sub-millisecond nanos, which getTime() would drop.
    if (value instanceof Timestamp t) {
      return t;
    }
    return new Timestamp(toEpochMillis(value, columnIndex, "Timestamp"));
  }

  @Override
  @Nullable
  public Timestamp getTimestamp(final String columnLabel, @Nullable final Calendar cal) throws SQLException
  {
    return getTimestamp(findColumn(columnLabel), cal);
  }

  @Override
  public URL getURL(final int columnIndex) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getURL not supported");
  }

  @Override
  public URL getURL(final String columnLabel) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getURL not supported");
  }

  @Override
  public void updateRef(final int columnIndex, final Ref x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateRef(final String columnLabel, final Ref x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBlob(final int columnIndex, final Blob x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBlob(final String columnLabel, final Blob x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateClob(final int columnIndex, final Clob x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateClob(final String columnLabel, final Clob x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateArray(final int columnIndex, final Array x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateArray(final String columnLabel, final Array x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public RowId getRowId(final int columnIndex) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getRowId not supported");
  }

  @Override
  public RowId getRowId(final String columnLabel) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getRowId not supported");
  }

  @Override
  public void updateRowId(final int columnIndex, final RowId x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateRowId(final String columnLabel, final RowId x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public int getHoldability()
  {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public boolean isClosed() throws SQLException
  {
    return closed.get();
  }

  @Override
  public void updateNString(final int columnIndex, final String nString) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateNString(final String columnLabel, final String nString) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateNClob(final int columnIndex, final NClob nClob) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateNClob(final String columnLabel, final NClob nClob) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public NClob getNClob(final int columnIndex) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getNClob not supported");
  }

  @Override
  public NClob getNClob(final String columnLabel) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getNClob not supported");
  }

  @Override
  public SQLXML getSQLXML(final int columnIndex) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getSQLXML not supported");
  }

  @Override
  public SQLXML getSQLXML(final String columnLabel) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getSQLXML not supported");
  }

  @Override
  public void updateSQLXML(final int columnIndex, final SQLXML xmlObject) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateSQLXML(final String columnLabel, final SQLXML xmlObject) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public String getNString(final int columnIndex) throws SQLException
  {
    return getString(columnIndex);
  }

  @Override
  public String getNString(final String columnLabel) throws SQLException
  {
    return getString(columnLabel);
  }

  @Override
  public Reader getNCharacterStream(final int columnIndex) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getNCharacterStream not supported");
  }

  @Override
  public Reader getNCharacterStream(final String columnLabel) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getNCharacterStream not supported");
  }

  @Override
  public void updateNCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateNCharacterStream(final String columnLabel, final Reader reader, final long length)
      throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateAsciiStream(final int columnIndex, final InputStream x, final long length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBinaryStream(final int columnIndex, final InputStream x, final long length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateAsciiStream(final String columnLabel, final InputStream x, final long length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBinaryStream(final String columnLabel, final InputStream x, final long length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateCharacterStream(final String columnLabel, final Reader reader, final long length)
      throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBlob(final int columnIndex, final InputStream inputStream, final long length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBlob(final String columnLabel, final InputStream inputStream, final long length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateClob(final int columnIndex, final Reader reader, final long length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateClob(final String columnLabel, final Reader reader, final long length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateNClob(final int columnIndex, final Reader reader, final long length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateNClob(final String columnLabel, final Reader reader, final long length) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateNCharacterStream(final int columnIndex, final Reader x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateNCharacterStream(final String columnLabel, final Reader reader) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateAsciiStream(final int columnIndex, final InputStream x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBinaryStream(final int columnIndex, final InputStream x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateCharacterStream(final int columnIndex, final Reader x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateAsciiStream(final String columnLabel, final InputStream x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBinaryStream(final String columnLabel, final InputStream x) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateCharacterStream(final String columnLabel, final Reader reader) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBlob(final int columnIndex, final InputStream inputStream) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateBlob(final String columnLabel, final InputStream inputStream) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateClob(final int columnIndex, final Reader reader) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateClob(final String columnLabel, final Reader reader) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateNClob(final int columnIndex, final Reader reader) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  public void updateNClob(final String columnLabel, final Reader reader) throws SQLException
  {
    throw updatesNotSupported();
  }

  @Override
  @Nullable
  @SuppressWarnings("unchecked")
  public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException
  {
    final Object value = getCurrentRowValue(columnIndex);
    if (value == null) {
      return null;
    }

    final Object coercedValue = coerceForGetObject(value, columnIndex);
    if (type.isInstance(coercedValue)) {
      return (T) coercedValue;
    }

    // From this point on, we know the value is non-null.
    if (type == String.class) {
      return (T) coerceToString(value);
    }
    if (type == Long.class) {
      return (T) Long.valueOf(toNumber(value, columnIndex, "long").longValue());
    }
    if (type == Integer.class) {
      return (T) Integer.valueOf((int) toNumber(value, columnIndex, "int").longValue());
    }
    if (type == Double.class) {
      return (T) Double.valueOf(toNumber(value, columnIndex, "double").doubleValue());
    }
    if (type == Float.class) {
      return (T) Float.valueOf((float) toNumber(value, columnIndex, "float").doubleValue());
    }
    if (type == Short.class) {
      return (T) Short.valueOf((short) toNumber(value, columnIndex, "short").longValue());
    }
    if (type == Byte.class) {
      return (T) Byte.valueOf((byte) toNumber(value, columnIndex, "byte").longValue());
    }
    if (type == Boolean.class) {
      return (T) Boolean.valueOf(toBoolean(value, columnIndex));
    }
    if (type == BigDecimal.class) {
      return (T) toBigDecimal(value, columnIndex);
    }
    if (type == Timestamp.class) {
      return (T) new Timestamp(toEpochMillis(value, columnIndex, "Timestamp"));
    }
    if (type == Date.class) {
      // No Calendar parameter on this method, so normalize in the default time zone, like getDate(int).
      return (T) toDate(toEpochMillis(value, columnIndex, "Date"), zoneOf(null));
    }
    if (type == Time.class) {
      return (T) toTime(toEpochMillis(value, columnIndex, "Time"), zoneOf(null));
    }
    if (type == byte[].class) {
      return (T) toBytes(value, columnIndex);
    }

    throw new DruidJdbcException(
        "Cannot convert column %s of type %s to requested type %s",
        columnIndex,
        value.getClass().getName(),
        type.getName()
    );
  }

  @Override
  public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException
  {
    return getObject(findColumn(columnLabel), type);
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException
  {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new DruidJdbcException("Cannot unwrap to class[%s]", iface.getName());
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface)
  {
    return iface.isAssignableFrom(getClass());
  }

  private void throwIfClosed() throws SQLException
  {
    if (closed.get()) {
      throw new DruidJdbcException("ResultSet is closed");
    }
  }

  private void validateRowPosition() throws SQLException
  {
    if (currentRow == null) {
      throw new DruidJdbcException("No current row. Call next() first.");
    }
  }

  private void validateColumnIndex(final int columnIndex) throws SQLException
  {
    final int columnCount = resultsIterator.getColumns().size();
    if (columnIndex < 1 || columnIndex > columnCount) {
      throw new DruidJdbcException("Invalid column index[%s]. Valid range[1-%s]", columnIndex, columnCount);
    }
  }

  /**
   * Returns the value of a column of the current row, and updates {@link #lastValueWasNull} so that
   * {@link #wasNull()} reflects this read.
   */
  @Nullable
  private Object getCurrentRowValue(final int columnIndex) throws SQLException
  {
    throwIfClosed();
    validateRowPosition();
    validateColumnIndex(columnIndex);

    final Object value = currentRow[columnIndex - 1]; // JDBC columns are 1-based
    lastValueWasNull = value == null;
    return value;
  }

  /**
   * Reads a column value for the numeric getters, which narrow the result with a cast: values too large for the
   * narrower type wrap around rather than throwing.
   *
   * @return the column value, or null if the column value is null
   */
  @Nullable
  private Number getNumericValue(final int columnIndex, final String targetLabel) throws SQLException
  {
    final Object value = getCurrentRowValue(columnIndex);
    if (value == null) {
      return null;
    }
    return toNumber(value, columnIndex, targetLabel);
  }

  /**
   * Coerces VARCHAR and ARRAY column value to the Java type that the {@code getObject} getters return for the
   * column's SQL type, which is the type {@link DruidResultSetMetaData#getColumnClassName(int)} reports. Values that
   * {@link QueryResultsIterator} already read as the right type are returned as-is.
   */
  @Nullable
  private Object coerceForGetObject(@Nullable final Object value, final int columnIndex) throws SQLException
  {
    if (value == null) {
      return null;
    }

    return switch (resultsIterator.getColumns().get(columnIndex - 1).jdbcType()) {
      case Types.VARCHAR, Types.CHAR -> coerceToString(value);
      case Types.ARRAY -> coerceToArray(value, columnIndex);
      default -> value;
    };
  }

  /**
   * Converts a non-null column value to a string. Lists, which Druid uses for arrays and for multi-value
   * dimensions, are written as JSON rather than using {@link Object#toString()}, so that the result is
   * unambiguous and matches how Druid stringifies these values server-side.
   */
  private String coerceToString(final Object value) throws SQLException
  {
    if (value instanceof List || value instanceof Map) {
      try {
        return jsonMapper.writeValueAsString(value);
      }
      catch (JsonProcessingException e) {
        throw new DruidJdbcException(e, "Cannot convert value of type[%s] to String", value.getClass().getName());
      }
    }

    return value.toString();
  }

  /**
   * Converts a non-null column value to an {@link Array}, for {@link #getArray(int)} and for {@code getObject}.
   */
  private Array coerceToArray(final Object value, final int columnIndex) throws SQLException
  {
    if (value instanceof List<?> list) {
      final JDBCType elementType = resultsIterator.getColumns().get(columnIndex - 1).arrayElementType();
      return new DruidArray(elementType.getVendorTypeNumber(), elementType.getName(), list.toArray());
    }
    throw cannotConvert(columnIndex, value, "Array");
  }

  /**
   * Converts a non-null column value for the numeric getters. Strings are not parsed; only {@link Number}
   * is accepted.
   *
   * @param targetLabel target type name used in error messages, e.g. {@code "int"}
   */
  private static Number toNumber(final Object value, final int columnIndex, final String targetLabel)
      throws SQLException
  {
    if (value instanceof Number n) {
      return n;
    }
    throw cannotConvert(columnIndex, value, targetLabel);
  }

  /**
   * Converts a non-null column value for {@link #getBoolean(int)}. Accepts {@link Boolean}, {@link Number}
   * (nonzero is true), and the string forms that {@link BooleanUtils} recognizes plus {@code "1"}.
   */
  private static boolean toBoolean(final Object value, final int columnIndex) throws SQLException
  {
    if (value instanceof Boolean b) {
      return b;
    }
    if (value instanceof Number n) {
      return n.intValue() != 0;
    }
    if (value instanceof String str) {
      return BooleanUtils.isBooleanTrue(str) || "1".equals(str);
    }
    throw cannotConvert(columnIndex, value, "boolean");
  }

  /**
   * Converts a non-null column value for {@link #getBigDecimal(int)}.
   */
  private static BigDecimal toBigDecimal(final Object value, final int columnIndex) throws SQLException
  {
    if (value instanceof BigDecimal bd) {
      return bd;
    }
    if (value instanceof Number) {
      return new BigDecimal(value.toString());
    }
    throw cannotConvert(columnIndex, value, "BigDecimal");
  }

  /**
   * Converts a non-null column value for {@link #getBytes(int)}.
   */
  private static byte[] toBytes(final Object value, final int columnIndex) throws SQLException
  {
    if (value instanceof byte[] bytes) {
      return bytes;
    }
    if (value instanceof String str) {
      // Druid's SQL JSON response encodes VARBINARY and COMPLEX column values as base64 strings
      // (Jackson's default binary serialization).
      try {
        return Base64.getDecoder().decode(str);
      }
      catch (IllegalArgumentException e) {
        throw new DruidJdbcException(e, "Cannot convert column %s value to bytes: not valid base64", columnIndex);
      }
    }
    throw cannotConvert(columnIndex, value, "bytes");
  }

  /**
   * Converts an already-materialized, non-null column value to epoch milliseconds, for the
   * {@code getDate}/{@code getTime}/{@code getTimestamp} getters. Numbers are treated as epoch millis.
   *
   * @param targetLabel JDBC type name used in error messages, e.g. {@code "Date"}
   */
  private static long toEpochMillis(final Object value, final int columnIndex, final String targetLabel)
      throws SQLException
  {
    if (value instanceof java.util.Date d) {
      return d.getTime();
    }
    if (value instanceof Number n) {
      return n.longValue();
    }
    if (value instanceof String str) {
      try {
        return DruidTimestampParser.parse(str).getTime();
      }
      catch (Exception e) {
        throw new DruidJdbcException(e, "Cannot convert '%s' to %s", value, targetLabel);
      }
    }
    throw cannotConvert(columnIndex, value, targetLabel);
  }

  /**
   * The time zone in which the {@code getDate} and {@code getTime} getters normalize their result. This is
   * {@code cal}'s time zone, or the JVM default time zone when {@code cal} is null, as the JDBC spec requires
   * of the getters that take no {@link Calendar}.
   */
  private static ZoneId zoneOf(@Nullable final Calendar cal)
  {
    return cal != null ? cal.getTimeZone().toZoneId() : TimeZone.getDefault().toZoneId();
  }

  /**
   * Converts epoch milliseconds to a {@link Date}, truncated to midnight of the calendar day that
   * {@code epochMillis} falls on in {@code zone}. {@link Date} requires this: its contract is that the
   * time-of-day fields of the wrapped millisecond value read as zero in the time zone the value is used with.
   */
  private static Date toDate(final long epochMillis, final ZoneId zone)
  {
    final LocalDate localDate = Instant.ofEpochMilli(epochMillis).atZone(zone).toLocalDate();
    return new Date(localDate.atStartOfDay(zone).toInstant().toEpochMilli());
  }

  /**
   * Converts epoch milliseconds to a {@link Time} holding the time of day that {@code epochMillis} falls on in
   * {@code zone}, placed on 1970-01-01. {@link Time} requires this: its contract is that the date fields of the
   * wrapped millisecond value read as the epoch day in the time zone the value is used with.
   */
  private static Time toTime(final long epochMillis, final ZoneId zone)
  {
    final LocalTime localTime = Instant.ofEpochMilli(epochMillis).atZone(zone).toLocalTime();
    return new Time(localTime.atDate(LocalDate.EPOCH).atZone(zone).toInstant().toEpochMilli());
  }

  private static DruidJdbcException cannotConvert(
      final int columnIndex,
      final Object value,
      final String targetLabel
  )
  {
    return new DruidJdbcException(
        "Cannot convert column %s of type %s to %s", columnIndex, value.getClass().getSimpleName(), targetLabel
    );
  }

  /**
   * Thrown by the {@code updateXxx} and other cursor-mutating methods, since this resultset is read-only.
   */
  private static DruidJdbcFeatureNotSupportedException updatesNotSupported()
  {
    return new DruidJdbcFeatureNotSupportedException("Updates not supported");
  }
}
