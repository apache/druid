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

package org.apache.druid.cli;

import com.google.common.io.BaseEncoding;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.skife.jdbi.v2.TransactionCallback;

import javax.annotation.Nullable;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Exports metadata tables to CSV files, reading the rows from any database through the JDBC connection of a
 * {@link SQLMetadataConnector}.
 *
 * A table is read and written in a single streaming pass: every row is read from the {@link ResultSet}, handed to a
 * {@link ValueConverter} which applies the conversions the {@code export-metadata} tool needs, and written as one CSV
 * record. The connector only provides the connection, the schema of the Druid tables and the fetch size that makes
 * the database stream its results; all CSV concerns live here.
 *
 * The output follows RFC 4180: a field containing a comma, a double quote or a line break is wrapped in double
 * quotes, with inner double quotes doubled, and a backslash is an ordinary character. A field which begins or ends
 * with whitespace is quoted as well, which RFC 4180 does not require but Derby needs in order to preserve it. A
 * NULL is written as an unquoted empty field while an empty string is written as a quoted empty field, so that the
 * two remain distinguishable by the import commands documented in {@code docs/operations/export-metadata.md}.
 */
public class MetadataCsvExporter
{
  /**
   * The kind of a column, as far as the CSV representation of its values is concerned.
   */
  public enum ColumnKind
  {
    /** A binary column, whose values are passed to the converter as an upper-case hexadecimal string. */
    BINARY,
    /** A boolean column, whose values are passed to the converter as {@code "true"} or {@code "false"}. */
    BOOLEAN,
    /** Any other column, whose values are passed to the converter as returned by {@link ResultSet#getString}. */
    OTHER
  }

  /**
   * Converts one value read from the database into the value to write to the CSV.
   *
   * The value is null for a NULL and is never escaped, so that a converter does not have to know about the CSV
   * format: escaping is applied to whatever it returns.
   */
  @FunctionalInterface
  public interface ValueConverter
  {
    @Nullable
    String convert(ColumnKind kind, @Nullable String value) throws Exception;
  }

  private final SQLMetadataConnector connector;

  public MetadataCsvExporter(final SQLMetadataConnector connector)
  {
    this.connector = connector;
  }

  /**
   * Exports the given columns of the given table to a CSV file, one record per row, in the order the columns are
   * given, applying {@code converter} to every value.
   */
  public void exportTable(
      final String tableName,
      final List<String> columns,
      final Path outputPath,
      final ValueConverter converter
  )
  {
    // Use a transaction so that the connection has autoCommit=false. PostgreSQL JDBC requires autoCommit=false and
    // a positive fetch size to use cursor-based streaming instead of buffering the entire ResultSet.
    connector.retryTransaction(
        (TransactionCallback<Void>) (handle, status) -> {
          final Connection conn = handle.getConnection();
          try (Statement stmt = conn.createStatement()) {
            // Set the fetch size unconditionally: some drivers use a sentinel value to request streaming, such as
            // Integer.MIN_VALUE in MySQL, which would be discarded by a positive-value check.
            stmt.setFetchSize(connector.getStreamingFetchSize());
            try (
                ResultSet rs = stmt.executeQuery(makeSelectStatement(conn, tableName, columns));
                Writer writer = new OutputStreamWriter(
                    Files.newOutputStream(outputPath),
                    StandardCharsets.UTF_8
                )
            ) {
              final ColumnKind[] kinds = readColumnKinds(rs.getMetaData());
              while (rs.next()) {
                writeRecord(writer, rs, kinds, converter);
              }
            }
          }
          return null;
        },
        SQLMetadataConnector.QUIET_RETRIES,
        SQLMetadataConnector.DEFAULT_MAX_TRIES
    );
  }

  /**
   * Builds the select statement for an export, qualifying the table with the schema that Druid's tables live in,
   * which is not necessarily the schema an unqualified name resolves to for this connection.
   *
   * The schema and the columns are quoted, since they are the names as stored in the database, so that reserved
   * words such as {@code end} work. The table name is left unquoted, so that it is folded by the database in the
   * same way as in every other Druid statement.
   */
  private String makeSelectStatement(
      final Connection conn,
      final String tableName,
      final List<String> columns
  ) throws SQLException
  {
    final String quote = conn.getMetaData().getIdentifierQuoteString();
    final String selectList = columns.stream()
                                     .map(column -> quoteIdentifier(quote, column))
                                     .collect(Collectors.joining(","));
    final String schema = connector.getMetadataTableSchema(conn);
    final String qualifiedTableName =
        schema == null ? tableName : quoteIdentifier(quote, schema) + "." + tableName;
    return StringUtils.format("SELECT %s FROM %s", selectList, qualifiedTableName);
  }

  /**
   * Quotes an identifier with the given identifier quote string of the database, doubling any occurrence of the
   * quote string inside the identifier. Returns the identifier unchanged if the database does not support quoting,
   * which {@link java.sql.DatabaseMetaData#getIdentifierQuoteString()} reports as a space.
   */
  private static String quoteIdentifier(@Nullable final String quote, final String identifier)
  {
    if (quote == null || " ".equals(quote)) {
      return identifier;
    }
    return quote + StringUtils.replace(identifier, quote, quote + quote) + quote;
  }

  private static ColumnKind[] readColumnKinds(final ResultSetMetaData meta) throws SQLException
  {
    final ColumnKind[] kinds = new ColumnKind[meta.getColumnCount()];
    for (int i = 0; i < kinds.length; i++) {
      final int type = meta.getColumnType(i + 1);
      if (type == Types.BINARY || type == Types.VARBINARY || type == Types.LONGVARBINARY || type == Types.BLOB
          // PostgreSQL reports BYTEA as OTHER
          || (type == Types.OTHER && "bytea".equalsIgnoreCase(meta.getColumnTypeName(i + 1)))) {
        kinds[i] = ColumnKind.BINARY;
      } else if (type == Types.BOOLEAN || type == Types.BIT) {
        kinds[i] = ColumnKind.BOOLEAN;
      } else {
        kinds[i] = ColumnKind.OTHER;
      }
    }
    return kinds;
  }

  @Nullable
  private static String readValue(final ResultSet rs, final int column, final ColumnKind kind) throws SQLException
  {
    switch (kind) {
      case BINARY:
        final byte[] bytes = rs.getBytes(column);
        return bytes == null ? null : BaseEncoding.base16().encode(bytes);
      case BOOLEAN:
        final boolean value = rs.getBoolean(column);
        return rs.wasNull() ? null : String.valueOf(value);
      default:
        return rs.getString(column);
    }
  }

  /**
   * Converts and writes the current row as one CSV record, leaving a NULL as null on the way to the converter so
   * that it stays distinguishable from an empty string.
   */
  private static void writeRecord(
      final Writer writer,
      final ResultSet rs,
      final ColumnKind[] kinds,
      final ValueConverter converter
  ) throws Exception
  {
    final StringBuilder record = new StringBuilder();
    for (int i = 0; i < kinds.length; i++) {
      if (i > 0) {
        record.append(',');
      }
      record.append(csvEscape(converter.convert(kinds[i], readValue(rs, i + 1, kinds[i]))));
    }
    writer.write(record.append('\n').toString());
  }

  /**
   * Escapes a value as one CSV field, as per RFC 4180.
   */
  static String csvEscape(@Nullable final String value)
  {
    if (value == null) {
      return "";
    } else if (value.isEmpty()) {
      return "\"\"";
    } else if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")
               // Derby strips leading and trailing spaces from an unquoted field, which would silently corrupt a
               // value such as a datasource name with a boundary space
               || Character.isWhitespace(value.charAt(0))
               || Character.isWhitespace(value.charAt(value.length() - 1))) {
      return "\"" + StringUtils.replace(value, "\"", "\"\"") + "\"";
    } else {
      return value;
    }
  }
}
