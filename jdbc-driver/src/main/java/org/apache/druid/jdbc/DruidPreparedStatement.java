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

import org.apache.druid.jdbc.http.QueryResultsIterator;
import org.apache.druid.jdbc.http.SqlParameter;
import org.apache.druid.jdbc.http.SqlRequest;
import org.apache.druid.jdbc.sql.SqlScanner;

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
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Our implementation of JDBC {@link PreparedStatement}.
 *
 * <p><b>Thread safety:</b> same contract as {@link DruidStatement}.
 */
public class DruidPreparedStatement extends DruidStatement implements PreparedStatement
{
  /**
   * ISO-8601 date formatter ({@code yyyy-MM-dd}), no timezone included.
   */
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

  /**
   * ISO-8601 time formatter ({@code HH:mm:ss[.SSS]}), no timezone included.
   */
  private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_TIME;

  /**
   * ISO-8601 timestamp formatter for instants in UTC ({@code yyyy-MM-dd'T'HH:mm:ss.SSS'Z'}).
   */
  private static final DateTimeFormatter TIMESTAMP_UTC_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ENGLISH).withZone(ZoneId.of("UTC"));

  /**
   * ISO-8601 zoneless timestamp formatter ({@code yyyy-MM-dd'T'HH:mm:ss[.SSS]}).
   */
  private static final DateTimeFormatter TIMESTAMP_ZONELESS_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

  /**
   * Typed NULL used to stand in for an unbound parameter in {@link #getMetaData()}.
   */
  private static final SqlParameter UNBOUND_PARAMETER_PLACEHOLDER =
      new SqlParameter(JDBCType.NULL.getName(), null);

  private final SqlScanner sqlScanner;
  private final Map<Integer, SqlParameter> parameters = new HashMap<>();
  private ResultSetMetaData cachedMetaData;

  public DruidPreparedStatement(final DruidConnection connection, final String sql) throws SQLException
  {
    super(connection);
    this.sqlScanner = SqlScanner.scan(sql);
  }

  @Override
  public ResultSet executeQuery() throws SQLException
  {
    throwIfClosed();

    final String sql = preparedSqlStatement();

    // Check that all expected parameters are set.
    final List<SqlParameter> parameterList = new ArrayList<>();
    for (int i = 1; i <= sqlScanner.getParameterCount(); i++) {
      final SqlParameter param = parameters.get(i);
      if (param == null) {
        throw new DruidJdbcException("Parameter[%s] must be bound", i);
      }
      parameterList.add(param);
    }

    return executeSql(sql, parameterList);
  }

  /**
   * Returns the SQL statement to send to the server, validating that there is exactly one and that it is not a
   * {@code SET} statement. Shared by {@link #executeQuery()} and {@link #getMetaData()}.
   */
  private String preparedSqlStatement() throws SQLException
  {
    if (!sqlScanner.getSetStatements().isEmpty()) {
      throw new DruidJdbcException("Cannot prepare SET statements");
    }

    final String sql = sqlScanner.getSqlStatement();
    if (sql == null) {
      throw new DruidJdbcException("No SQL statement to execute");
    }

    return sql;
  }

  @Override
  public int executeUpdate() throws SQLException
  {
    throwIfClosed();
    throw new DruidJdbcFeatureNotSupportedException("Update operations are not supported");
  }

  @Override
  public ResultSet executeQuery(final String sql) throws SQLException
  {
    throw cannotUseSqlArgument("executeQuery");
  }

  @Override
  public int executeUpdate(final String sql) throws SQLException
  {
    throw cannotUseSqlArgument("executeUpdate");
  }

  @Override
  public boolean execute(final String sql) throws SQLException
  {
    throw cannotUseSqlArgument("execute");
  }

  @Override
  public void addBatch(final String sql) throws SQLException
  {
    throw cannotUseSqlArgument("addBatch");
  }

  @Override
  public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException
  {
    throw cannotUseSqlArgument("executeUpdate");
  }

  @Override
  public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException
  {
    throw cannotUseSqlArgument("executeUpdate");
  }

  @Override
  public int executeUpdate(final String sql, final String[] columnNames) throws SQLException
  {
    throw cannotUseSqlArgument("executeUpdate");
  }

  @Override
  public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException
  {
    throw cannotUseSqlArgument("execute");
  }

  @Override
  public boolean execute(final String sql, final int[] columnIndexes) throws SQLException
  {
    throw cannotUseSqlArgument("execute");
  }

  @Override
  public boolean execute(final String sql, final String[] columnNames) throws SQLException
  {
    throw cannotUseSqlArgument("execute");
  }

  @Override
  public void setNull(final int parameterIndex, final int sqlType) throws SQLException
  {
    checkParameterIndex(parameterIndex);
    final String typeName;
    try {
      typeName = JDBCType.valueOf(sqlType).getName();
    }
    catch (IllegalArgumentException e) {
      throw new DruidJdbcException(e, "Unsupported SQL type[%s]", sqlType);
    }
    parameters.put(parameterIndex, new SqlParameter(typeName, null));
  }

  @Override
  public void setBoolean(final int parameterIndex, final boolean x) throws SQLException
  {
    checkParameterIndex(parameterIndex);
    parameters.put(parameterIndex, new SqlParameter(JDBCType.BOOLEAN.getName(), x));
  }

  @Override
  public void setByte(final int parameterIndex, final byte x) throws SQLException
  {
    checkParameterIndex(parameterIndex);
    parameters.put(parameterIndex, new SqlParameter(JDBCType.TINYINT.getName(), x));
  }

  @Override
  public void setShort(final int parameterIndex, final short x) throws SQLException
  {
    checkParameterIndex(parameterIndex);
    parameters.put(parameterIndex, new SqlParameter(JDBCType.SMALLINT.getName(), x));
  }

  @Override
  public void setInt(final int parameterIndex, final int x) throws SQLException
  {
    checkParameterIndex(parameterIndex);
    parameters.put(parameterIndex, new SqlParameter(JDBCType.INTEGER.getName(), x));
  }

  @Override
  public void setLong(final int parameterIndex, final long x) throws SQLException
  {
    checkParameterIndex(parameterIndex);
    parameters.put(parameterIndex, new SqlParameter(JDBCType.BIGINT.getName(), x));
  }

  @Override
  public void setFloat(final int parameterIndex, final float x) throws SQLException
  {
    checkParameterIndex(parameterIndex);
    parameters.put(parameterIndex, new SqlParameter(JDBCType.REAL.getName(), x));
  }

  @Override
  public void setDouble(final int parameterIndex, final double x) throws SQLException
  {
    checkParameterIndex(parameterIndex);
    parameters.put(parameterIndex, new SqlParameter(JDBCType.DOUBLE.getName(), x));
  }

  @Override
  public void setBigDecimal(final int parameterIndex, final BigDecimal x) throws SQLException
  {
    checkParameterIndex(parameterIndex);
    parameters.put(parameterIndex, new SqlParameter(JDBCType.DECIMAL.getName(), x));
  }

  @Override
  public void setString(final int parameterIndex, final String x) throws SQLException
  {
    checkParameterIndex(parameterIndex);
    parameters.put(parameterIndex, new SqlParameter(JDBCType.VARCHAR.getName(), x));
  }

  @Override
  public void setBytes(final int parameterIndex, final byte[] x) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setBytes not supported");
  }

  @Override
  public void setDate(final int parameterIndex, final Date x) throws SQLException
  {
    setDate(parameterIndex, x, null);
  }

  @Override
  public void setTime(final int parameterIndex, final Time x) throws SQLException
  {
    throw timeNotSupported("setTime");
  }

  @Override
  public void setTimestamp(final int parameterIndex, final Timestamp x) throws SQLException
  {
    setTimestamp(parameterIndex, x, null);
  }

  @Override
  public void setAsciiStream(final int parameterIndex, final InputStream x, final int length) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setAsciiStream not supported");
  }

  @Override
  public void setUnicodeStream(final int parameterIndex, final InputStream x, final int length) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setUnicodeStream not supported");
  }

  @Override
  public void setBinaryStream(final int parameterIndex, final InputStream x, final int length) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setBinaryStream not supported");
  }

  @Override
  public void clearParameters()
  {
    parameters.clear();
  }

  @Override
  public void setObject(final int parameterIndex, final Object x, final int targetSqlType) throws SQLException
  {
    setObject(parameterIndex, x, targetSqlType, -1);
  }

  @Override
  public void setObject(final int parameterIndex, final Object obj) throws SQLException
  {
    checkParameterIndex(parameterIndex);
    if (obj == null) {
      parameters.put(parameterIndex, new SqlParameter(JDBCType.VARCHAR.getName(), null));
    } else if (obj instanceof String s) {
      setString(parameterIndex, s);
    } else if (obj instanceof Integer i) {
      setInt(parameterIndex, i);
    } else if (obj instanceof Long l) {
      setLong(parameterIndex, l);
    } else if (obj instanceof Double d) {
      setDouble(parameterIndex, d);
    } else if (obj instanceof Float f) {
      setFloat(parameterIndex, f);
    } else if (obj instanceof Short sh) {
      setShort(parameterIndex, sh);
    } else if (obj instanceof Byte b) {
      setByte(parameterIndex, b);
    } else if (obj instanceof Boolean b) {
      setBoolean(parameterIndex, b);
    } else if (obj instanceof BigDecimal bd) {
      setBigDecimal(parameterIndex, bd);
    } else if (obj instanceof Timestamp ts) {
      setTimestamp(parameterIndex, ts);
    } else if (obj instanceof Date d) {
      setDate(parameterIndex, d);
    } else if (obj instanceof Time || obj instanceof LocalTime) {
      throw timeNotSupported("setObject");
    } else if (obj instanceof LocalDate) {
      parameters.put(parameterIndex, new SqlParameter(JDBCType.DATE.getName(), serializeParameterValue(obj)));
    } else if (obj instanceof LocalDateTime ||
               obj instanceof OffsetDateTime ||
               obj instanceof ZonedDateTime ||
               obj instanceof Instant) {
      parameters.put(parameterIndex, new SqlParameter(JDBCType.TIMESTAMP.getName(), serializeParameterValue(obj)));
    } else if (obj instanceof UUID) {
      parameters.put(parameterIndex, new SqlParameter(JDBCType.VARCHAR.getName(), serializeParameterValue(obj)));
    } else {
      throw new DruidJdbcFeatureNotSupportedException(
          "setObject does not support type[%s] (parameter[%s])", obj.getClass().getName(), parameterIndex
      );
    }
  }

  @Override
  public boolean execute() throws SQLException
  {
    //noinspection resource: caller is expected to call getCurrentResultSet()
    executeQuery();
    return true;
  }

  @Override
  public void addBatch() throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("Batch operations not supported");
  }

  @Override
  public void setCharacterStream(final int parameterIndex, final Reader reader, final int length) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setCharacterStream not supported");
  }

  @Override
  public void setRef(final int parameterIndex, final Ref x) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setRef not supported");
  }

  @Override
  public void setBlob(final int parameterIndex, final Blob x) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setBlob not supported");
  }

  @Override
  public void setClob(final int parameterIndex, final Clob x) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setClob not supported");
  }

  @Override
  public void setArray(final int parameterIndex, final Array x) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setArray not supported");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException
  {
    throwIfClosed();

    if (cachedMetaData != null) {
      return cachedMetaData;
    }

    final String sql = preparedSqlStatement();

    try {
      final Map<String, Object> queryContext = newQueryContext();
      queryContext.put("sqlOuterLimit", 0);

      // Fill unbound parameters (getMetaData does not require all parameters to be bound).
      final List<SqlParameter> parameterList = new ArrayList<>();
      for (int i = 1; i <= sqlScanner.getParameterCount(); i++) {
        parameterList.add(parameters.getOrDefault(i, UNBOUND_PARAMETER_PLACEHOLDER));
      }

      final SqlRequest request = SqlRequest.of(sql, queryContext, parameterList);
      try (final QueryResultsIterator results = getHttpClient().runQuery(request)) {
        cachedMetaData = new DruidResultSetMetaData(results.getColumns());
        return cachedMetaData;
      }
    }
    catch (SQLException e) {
      throw e;
    }
    catch (Exception e) {
      throw new DruidJdbcException(e, "Failed to get metadata: %s", e);
    }
  }

  @Override
  public void setDate(final int parameterIndex, final Date x, final Calendar cal) throws SQLException
  {
    checkParameterIndex(parameterIndex);
    final String value;
    if (x == null) {
      value = null;
    } else {
      final TimeZone tz = cal != null ? cal.getTimeZone() : TimeZone.getDefault();
      final LocalDate ld = Instant.ofEpochMilli(x.getTime()).atZone(tz.toZoneId()).toLocalDate();
      value = DATE_FORMATTER.format(ld);
    }
    parameters.put(parameterIndex, new SqlParameter(JDBCType.DATE.getName(), value));
  }

  @Override
  public void setTime(final int parameterIndex, final Time x, final Calendar cal) throws SQLException
  {
    throw timeNotSupported("setTime");
  }

  @Override
  public void setTimestamp(final int parameterIndex, final Timestamp x, final Calendar cal) throws SQLException
  {
    checkParameterIndex(parameterIndex);
    final Object value;
    if (x == null) {
      value = null;
    } else {
      value = serializeParameterValue(Instant.ofEpochMilli(x.getTime()));
    }
    parameters.put(parameterIndex, new SqlParameter(JDBCType.TIMESTAMP.getName(), value));
  }

  @Override
  public void setNull(final int parameterIndex, final int sqlType, final String typeName) throws SQLException
  {
    setNull(parameterIndex, sqlType);
  }

  @Override
  public void setURL(final int parameterIndex, final URL x) throws SQLException
  {
    setString(parameterIndex, x != null ? x.toString() : null);
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getParameterMetaData not supported");
  }

  @Override
  public void setRowId(final int parameterIndex, final RowId x) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setRowId not supported");
  }

  @Override
  public void setNString(final int parameterIndex, final String value) throws SQLException
  {
    setString(parameterIndex, value);
  }

  @Override
  public void setNCharacterStream(final int parameterIndex, final Reader value, final long length) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setNCharacterStream not supported");
  }

  @Override
  public void setNClob(final int parameterIndex, final NClob value) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setNClob not supported");
  }

  @Override
  public void setClob(final int parameterIndex, final Reader reader, final long length) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setClob not supported");
  }

  @Override
  public void setBlob(final int parameterIndex, final InputStream inputStream, final long length) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setBlob not supported");
  }

  @Override
  public void setNClob(final int parameterIndex, final Reader reader, final long length) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setNClob not supported");
  }

  @Override
  public void setSQLXML(final int parameterIndex, final SQLXML xmlObject) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setSQLXML not supported");
  }

  @Override
  public void setObject(final int parameterIndex, final Object x, final int targetSqlType, final int scaleOrLength)
      throws SQLException
  {
    checkParameterIndex(parameterIndex);

    final JDBCType jdbcType;
    try {
      jdbcType = JDBCType.valueOf(targetSqlType);
    }
    catch (IllegalArgumentException e) {
      throw new DruidJdbcException(e, "Unsupported SQL type[%s]", targetSqlType);
    }

    if (jdbcType == JDBCType.TIME || jdbcType == JDBCType.TIME_WITH_TIMEZONE) {
      throw timeNotSupported("setObject");
    }

    if (x == null || jdbcType == JDBCType.NULL) {
      setNull(parameterIndex, targetSqlType);
      return;
    }

    try {
      switch (jdbcType) {
        case BIT, BOOLEAN -> setBoolean(parameterIndex, castToBoolean(x));
        case TINYINT -> setByte(parameterIndex, castToNumber(x).byteValue());
        case SMALLINT -> setShort(parameterIndex, castToNumber(x).shortValue());
        case INTEGER -> setInt(parameterIndex, castToNumber(x).intValue());
        case BIGINT -> setLong(parameterIndex, castToNumber(x).longValue());
        case REAL -> setFloat(parameterIndex, castToNumber(x).floatValue());
        case FLOAT, DOUBLE -> setDouble(parameterIndex, castToNumber(x).doubleValue());
        case DECIMAL, NUMERIC -> setBigDecimal(parameterIndex, castToBigDecimal(x, scaleOrLength));
        case CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR, LONGNVARCHAR ->
          setString(parameterIndex, String.valueOf(serializeParameterValue(x)));
        case DATE -> setDate(
            parameterIndex,
            x instanceof String s ? Date.valueOf(s) : new Date(castToMillis(x))
        );
        case TIMESTAMP, TIMESTAMP_WITH_TIMEZONE -> setTimestamp(
            parameterIndex,
            x instanceof String s ? Timestamp.valueOf(s) : new Timestamp(castToMillis(x))
        );

        // Let the value's own class pick the wire type, as Avatica does for JAVA_OBJECT.
        case JAVA_OBJECT, OTHER -> setObject(parameterIndex, x);

        default -> throw new DruidJdbcFeatureNotSupportedException(
            "setObject does not support SQL type[%s] (parameter[%s])", jdbcType.getName(), parameterIndex
        );
      }
    }
    catch (IllegalArgumentException e) {
      throw new DruidJdbcException(
          e,
          DruidSQLState.InvalidParameterType,
          "Cannot convert an instance of[%s] to type[%s] (parameter[%s])",
          x.getClass().getName(),
          jdbcType.getName(),
          parameterIndex
      );
    }
  }

  @Override
  public void setAsciiStream(final int parameterIndex, final InputStream x, final long length) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setAsciiStream not supported");
  }

  @Override
  public void setBinaryStream(final int parameterIndex, final InputStream x, final long length) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setBinaryStream not supported");
  }

  @Override
  public void setCharacterStream(final int parameterIndex, final Reader reader, final long length) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setCharacterStream not supported");
  }

  @Override
  public void setAsciiStream(final int parameterIndex, final InputStream x) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setAsciiStream not supported");
  }

  @Override
  public void setBinaryStream(final int parameterIndex, final InputStream x) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setBinaryStream not supported");
  }

  @Override
  public void setCharacterStream(final int parameterIndex, final Reader reader) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setCharacterStream not supported");
  }

  @Override
  public void setNCharacterStream(final int parameterIndex, final Reader value) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setNCharacterStream not supported");
  }

  @Override
  public void setClob(final int parameterIndex, final Reader reader) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setClob not supported");
  }

  @Override
  public void setBlob(final int parameterIndex, final InputStream inputStream) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setBlob not supported");
  }

  @Override
  public void setNClob(final int parameterIndex, final Reader reader) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("setNClob not supported");
  }

  private void checkParameterIndex(final int parameterIndex) throws SQLException
  {
    throwIfClosed();

    if (parameterIndex < 1) {
      throw new DruidJdbcException("Parameter index must be >= 1, got: %s", parameterIndex);
    }

    final int expectedParameters = sqlScanner.getParameterCount();
    if (parameterIndex > expectedParameters) {
      throw new DruidJdbcException(
          "Parameter index %s is out of range; expected %s parameter(s)",
          parameterIndex,
          expectedParameters
      );
    }
  }

  private static DruidJdbcException cannotUseSqlArgument(final String method)
  {
    return new DruidJdbcException("%s with a SQL argument cannot be called on a PreparedStatement", method);
  }

  /**
   * Thrown by the {@code setTime} methods, and by the {@code setObject} overloads that would bind a SQL TIME.
   */
  private static DruidJdbcFeatureNotSupportedException timeNotSupported(final String method)
  {
    return new DruidJdbcFeatureNotSupportedException("%s not supported: Druid SQL has no TIME type.", method);
  }

  /**
   * Converts a bound value to a boolean.
   */
  private static boolean castToBoolean(final Object in)
  {
    if (in instanceof Boolean b) {
      return b;
    } else if (in instanceof String s) {
      if (BooleanUtils.isBooleanTrue(s)) {
        return true;
      } else if (BooleanUtils.isBooleanFalse(s)) {
        return false;
      }
    }

    throw new IllegalArgumentException("not a boolean");
  }

  /**
   * Converts a bound value to a {@link Number}.
   */
  private static Number castToNumber(final Object in)
  {
    if (in instanceof Number n) {
      return n;
    } else {
      return new BigDecimal(String.valueOf(in));
    }
  }

  /**
   * Converts a bound value to a {@link BigDecimal}, rounded to {@code scale} when it is nonnegative.
   */
  private static BigDecimal castToBigDecimal(final Object in, final int scale)
  {
    final Number number = castToNumber(in);
    final BigDecimal value = number instanceof BigDecimal bd ? bd : new BigDecimal(number.toString());
    return scale >= 0 ? value.setScale(scale, RoundingMode.HALF_UP) : value;
  }

  /**
   * Converts a non-string bound value to epoch milliseconds, for the {@code DATE} and {@code TIMESTAMP} targets.
   * Local values are resolved in the JVM default time zone, matching the {@code setDate} and {@code setTimestamp}
   * overloads that take no {@link Calendar}.
   */
  private static long castToMillis(final Object in)
  {
    if (in instanceof java.util.Date d) {
      return d.getTime();
    } else if (in instanceof Instant i) {
      return i.toEpochMilli();
    } else if (in instanceof OffsetDateTime odt) {
      return odt.toInstant().toEpochMilli();
    } else if (in instanceof ZonedDateTime zdt) {
      return zdt.toInstant().toEpochMilli();
    } else if (in instanceof LocalDateTime ldt) {
      return Timestamp.valueOf(ldt).getTime();
    } else if (in instanceof LocalDate ld) {
      return Date.valueOf(ld).getTime();
    } else {
      return castToNumber(in).longValue();
    }
  }

  /**
   * Converts a bound value to its JSON-serializable representation.
   */
  private static Object serializeParameterValue(final Object obj)
  {
    if (obj instanceof LocalDate) {
      return DATE_FORMATTER.format((LocalDate) obj);
    } else if (obj instanceof LocalTime) {
      return TIME_FORMATTER.format((LocalTime) obj);
    } else if (obj instanceof LocalDateTime) {
      return TIMESTAMP_ZONELESS_FORMATTER.format((LocalDateTime) obj);
    } else if (obj instanceof OffsetDateTime) {
      return TIMESTAMP_UTC_FORMATTER.format(((OffsetDateTime) obj).toInstant());
    } else if (obj instanceof ZonedDateTime) {
      return TIMESTAMP_UTC_FORMATTER.format(((ZonedDateTime) obj).toInstant());
    } else if (obj instanceof Instant) {
      return TIMESTAMP_UTC_FORMATTER.format((Instant) obj);
    } else if (obj instanceof UUID) {
      return obj.toString();
    } else {
      return obj;
    }
  }
}
