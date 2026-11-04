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

package org.apache.druid.jdbc.http;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.JsonEOFException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jdbc.DruidJdbcException;
import org.apache.druid.jdbc.DruidTimestampParser;
import org.apache.druid.jdbc.StringUtils;

import javax.annotation.Nullable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class QueryResultsIteratorImpl implements QueryResultsIterator
{
  private final JsonParser jsonParser;
  private final InputStream responseStream;
  private final List<ColumnMetadata> columns;

  /**
   * sqlQueryId of the query these results belong to, if known.
   */
  @Nullable
  private final String sqlQueryId;

  /**
   * Written by {@link #close()}, which may run on a different thread than the one doing the reading.
   */
  private volatile boolean closed = false;

  private boolean reachedEnd = false;

  /**
   * True when {@link #hasNext()} has advanced the parser to a START_ARRAY token for a data row
   * that has not yet been consumed by {@link #next()}. Used to make {@link #hasNext()} idempotent.
   */
  private boolean peekedRow = false;

  /**
   * Number of data rows returned by {@link #next()} so far.
   */
  private long rowsRead;

  public QueryResultsIteratorImpl(
      final InputStream responseStream,
      final ObjectMapper objectMapper,
      @Nullable final String sqlQueryId
  ) throws SQLException
  {
    this.sqlQueryId = sqlQueryId;
    this.responseStream = responseStream;

    try {
      final JsonFactory jsonFactory = objectMapper.getFactory();
      this.jsonParser = jsonFactory.createParser(responseStream);
      this.columns = new ArrayList<>();
      initialize();
    }
    catch (SQLException e) {
      throw e;
    }
    catch (Exception e) {
      throw readError(e, "Failed to create streaming iterator");
    }
  }

  @Override
  public boolean hasNext() throws SQLException
  {
    if (closed) {
      throw new DruidJdbcException("Result iterator has been closed");
    }

    if (reachedEnd) {
      return false;
    }

    if (peekedRow) {
      // Already advanced to a START_ARRAY for the next data row; don't advance again.
      return true;
    }

    try {
      final JsonToken nextToken = jsonParser.nextToken();
      if (nextToken == JsonToken.END_ARRAY) {
        // We've reached the end of the main array.
        reachedEnd = true;
        return false;
      } else if (nextToken == JsonToken.START_ARRAY) {
        // Start of a new data row.
        peekedRow = true;
        return true;
      } else {
        throw new DruidJdbcException("Expected array or end of array, got[%s]", nextToken);
      }
    }
    catch (SQLException e) {
      reachedEnd = true;
      throw e;
    }
    catch (Exception e) {
      reachedEnd = true;
      throw readError(e, "Error reading query results");
    }
  }

  @Override
  public Object[] next() throws SQLException
  {
    if (closed) {
      throw new DruidJdbcException("Result iterator has been closed");
    }

    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    try {
      peekedRow = false;
      return readDataRow();
    }
    catch (IOException e) {
      throw readError(e, "Failed to read next row");
    }
  }

  @Override
  public List<ColumnMetadata> getColumns()
  {
    return columns;
  }

  @Override
  public void close() throws IOException
  {
    closed = true;
    responseStream.close();
  }

  private void initialize() throws IOException, SQLException
  {
    // Expect the response to start with an array.
    final JsonToken firstToken = jsonParser.nextToken();
    if (firstToken == null) {
      throw truncatedResponse(null, 0, sqlQueryId);
    } else if (firstToken != JsonToken.START_ARRAY) {
      throw new DruidJdbcException("Expected array response format");
    }

    // Read and parse the header (column names) row.
    final List<String> headerRow;
    if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
      throw new DruidJdbcException("Response missing header row");
    } else {
      headerRow = readMetadataRow();
    }

    // Read and parse the typesHeader row.
    if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
      throw new DruidJdbcException("Response missing typesHeader row");
    }
    final List<String> nativeTypesRow = readMetadataRow();

    // Read and parse the sqlTypesHeader row.
    if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
      throw new DruidJdbcException("Response missing sqlTypesHeader row");
    }
    final List<String> sqlTypesRow = readMetadataRow();

    if (headerRow.size() != sqlTypesRow.size()) {
      throw new DruidJdbcException("header and sqlTypesHeader must have same number of columns");
    }

    if (nativeTypesRow.size() != sqlTypesRow.size()) {
      throw new DruidJdbcException("typesHeader and sqlTypesHeader must have same number of columns");
    }

    // Build column metadata.
    for (int i = 0; i < headerRow.size(); i++) {
      final JDBCType type = JDBCType.valueOf(sqlTypesRow.get(i));
      columns.add(new ColumnMetadata(headerRow.get(i), type, nativeTypesRow.get(i)));
    }
  }

  /**
   * Reads a metadata row (e.g. header, sqlTypesHeader) from the stream. Advances to the {@link JsonToken#END_ARRAY}
   * at the end of the row.
   */
  private List<String> readMetadataRow() throws IOException
  {
    final List<String> row = new ArrayList<>();
    JsonToken token;
    while ((token = jsonParser.nextToken()) != JsonToken.END_ARRAY) {
      if (token == JsonToken.VALUE_STRING) {
        row.add(jsonParser.getValueAsString());
      } else if (token == JsonToken.VALUE_NULL) {
        row.add(null);
      } else {
        row.add(jsonParser.getValueAsString());
      }
    }
    return row;
  }

  /**
   * Reads a data row from the stream. Coerces objects to the expected Java types using
   * {@link #readDataObject(String, String, int)}. Advances to the {@link JsonToken#END_ARRAY} at the end of the row.
   */
  private Object[] readDataRow() throws IOException, SQLException
  {
    final Object[] row = new Object[columns.size()];
    int columnIndex = 0;
    while ((jsonParser.nextToken()) != JsonToken.END_ARRAY) {
      if (columnIndex >= columns.size()) {
        throw new DruidJdbcException("Data row too long");
      }

      final ColumnMetadata column = columns.get(columnIndex);
      final Object value = readDataObject(column.name(), column.nativeType(), column.jdbcType());
      row[columnIndex++] = value;
    }

    if (columnIndex != columns.size()) {
      throw new DruidJdbcException("Data row too short");
    }

    rowsRead++;
    return row;
  }

  /**
   * Reads a data object from the stream. If the data object consists of multiple tokens, this advances to the
   * final token.
   *
   * @param columnName name of the column, used in error messages
   * @param nativeType Druid native type of the value, used to type the elements if the value is an array
   * @param jdbcType   SQL type that determines the Java type of the returned value. This is the column's own declared
   *                   type for a top-level value, or the element type for a value nested inside an array.
   *
   * @throws DruidJdbcException if the value cannot be read as the declared type
   */
  @Nullable
  private Object readDataObject(
      final String columnName,
      @Nullable final String nativeType,
      final int jdbcType
  ) throws IOException, SQLException
  {
    final JsonToken token = jsonParser.currentToken();
    if (token == JsonToken.VALUE_NULL) {
      return null;
    }

    // Handle nested values: read JSON arrays into Lists, and JSON objects into Maps.
    if (token == JsonToken.START_ARRAY) {
      return readJsonArray(columnName, ColumnMetadata.arrayElementNativeType(nativeType));
    } else if (token == JsonToken.START_OBJECT) {
      return jsonParser.readValueAs(Object.class);
    }

    // About to read a primitive value. Finish the token now, so a truncation is reported as a truncation rather
    // than cannot-coerce.
    jsonParser.finishToken();

    try {
      switch (jdbcType) {
        case Types.BIT:
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
          return jsonParser.getIntValue();

        case Types.BIGINT:
          return jsonParser.getLongValue();

        case Types.REAL:
          return (float) jsonParser.getDoubleValue();

        case Types.FLOAT:
        case Types.DOUBLE:
          return jsonParser.getDoubleValue();

        case Types.DECIMAL:
        case Types.NUMERIC:
          return jsonParser.getDecimalValue();

        case Types.BOOLEAN:
          return jsonParser.getValueAsBoolean();

        case Types.TIMESTAMP:
          if (token == JsonToken.VALUE_NUMBER_INT) {
            return new Timestamp(jsonParser.getLongValue());
          } else {
            return DruidTimestampParser.parse(jsonParser.getValueAsString());
          }

        case Types.DATE:
          // Keep the calendar day the server floored the value to, rather than the instant, so that the day does
          // not shift when the JVM default time zone differs from the session time zone. See
          // DruidTimestampParser#parseLocalDate.
          return Date.valueOf(DruidTimestampParser.parseLocalDate(jsonParser.getValueAsString()));

        case Types.TIME:
          if (token == JsonToken.VALUE_NUMBER_INT) {
            return new Time(jsonParser.getLongValue());
          } else {
            return new Time(DruidTimestampParser.parse(jsonParser.getValueAsString()).getTime());
          }

        case Types.VARCHAR:
        case Types.CHAR:
          return jsonParser.getValueAsString();

        default:
          break;
      }
    }
    catch (Exception e) {
      throw cannotCoerce(columnName, jdbcType, e);
    }

    // Other type: let Jackson choose the representation.
    return jsonParser.readValueAs(Object.class);
  }

  /**
   * Returns the exception to throw when a value from the server cannot be read as its column's declared SQL type.
   */
  private DruidJdbcException cannotCoerce(
      final String columnName,
      final int sqlType,
      @Nullable final Throwable cause
  ) throws IOException
  {
    final String format = "Cannot read value[%s] of column[%s] as declared type[%s]";
    final String valueText = jsonParser.getText();
    final JDBCType jdbcType = JDBCType.valueOf(sqlType);
    if (cause == null) {
      return new DruidJdbcException(format, valueText, columnName, jdbcType);
    } else {
      return new DruidJdbcException(cause, format, valueText, columnName, jdbcType);
    }
  }

  /**
   * Reads a data array from the stream. Advances to the {@link JsonToken#END_ARRAY} at the end of the data array.
   *
   * @param columnName        name of the column, used in error messages
   * @param elementNativeType Druid native type of the elements of this array
   */
  private List<Object> readJsonArray(
      final String columnName,
      @Nullable final String elementNativeType
  ) throws IOException, SQLException
  {
    final int elementJdbcType = ColumnMetadata.jdbcTypeForNativeType(elementNativeType);
    final List<Object> retVal = new ArrayList<>();
    while ((jsonParser.nextToken()) != JsonToken.END_ARRAY) {
      retVal.add(readDataObject(columnName, elementNativeType, elementJdbcType));
    }
    return retVal;
  }

  /**
   * Returns the exception to throw when reading the response stream failed. The single place that decides whether
   * a read failure was the server cutting us off, or something else.
   *
   * @param e    the exception that reading threw
   * @param what what the driver was doing, used as the message prefix for a failure that is not a truncation
   */
  private DruidJdbcException readError(final Throwable e, final String what)
  {
    if (isResponseTruncated(e)) {
      return truncatedResponse(e, rowsRead, sqlQueryId);
    } else {
      return new DruidJdbcException(e, "%s: %s", what, e);
    }
  }

  /**
   * Returns whether an exception thrown while reading the response stream means the response body ended before
   * the server finished writing it.
   */
  private static boolean isResponseTruncated(@Nullable final Throwable e)
  {
    Throwable t = e;
    while (t != null) {
      if (t instanceof JsonEOFException || t instanceof EOFException) {
        return true;
      } else {
        t = t.getCause();
      }
    }

    return false;
  }

  /**
   * Returns the exception to throw when the response was truncated.
   *
   * @param cause      the exception that revealed the truncation, if there was one
   * @param rowsRead   number of rows successfully delivered before the response was cut off
   * @param sqlQueryId sqlQueryId to point the user at in the server's logs, if known
   */
  private static DruidJdbcException truncatedResponse(
      @Nullable final Throwable cause,
      final long rowsRead,
      @Nullable final String sqlQueryId
  )
  {
    final String format =
        "Truncated response after[%,d] rows: the server may have cut off the response because the query hit an "
        + "error or timed out. Check the Druid broker logs for %s.";
    final String logHint =
        sqlQueryId == null ? "details" : StringUtils.format("sqlQueryId[%s]", sqlQueryId);

    if (cause == null) {
      return new DruidJdbcException(format, rowsRead, logHint);
    } else {
      return new DruidJdbcException(cause, format, rowsRead, logHint);
    }
  }
}
