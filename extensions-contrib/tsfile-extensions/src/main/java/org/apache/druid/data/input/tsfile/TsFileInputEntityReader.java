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

package org.apache.druid.data.input.tsfile;

import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reads the rows of a TsFile file with the TsFile table-model reader and converts them into
 * {@link MapBasedInputRow}-compatible maps.
 * <p>
 * TsFile stores its own timestamp, which is exposed to Druid under the name {@value TsFileInputFormat#TIME_COLUMN_NAME}.
 * Tag, field, and attribute columns are exposed as-is.
 */
public class TsFileInputEntityReader extends IntermediateRowParsingReader<Map<String, Object>>
{
  private final InputRowSchema inputRowSchema;
  private final InputEntity source;
  @Nullable
  private final File temporaryDirectory;
  @Nullable
  private final List<String> tables;
  @Nullable
  private final List<String> columns;
  @Nullable
  private final Interval interval;

  TsFileInputEntityReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      @Nullable File temporaryDirectory,
      @Nullable List<String> tables,
      @Nullable List<String> columns,
      @Nullable Interval interval
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.source = source;
    this.temporaryDirectory = temporaryDirectory;
    this.tables = tables;
    this.columns = columns;
    this.interval = interval;
  }

  @Override
  protected CloseableIterator<Map<String, Object>> intermediateRowIterator() throws IOException
  {
    final InputEntity.CleanableFile materialized =
        source.fetch(temporaryDirectory, new byte[InputEntity.DEFAULT_FETCH_BUFFER_SIZE]);
    final ITsFileReader reader;
    try {
      reader = new TsFileReaderBuilder().file(materialized.file()).build();
    }
    catch (IOException | RuntimeException e) {
      materialized.close();
      throw e;
    }

    final List<String> tableNames;
    try {
      tableNames = resolveTableNames(reader);
    }
    catch (IOException | RuntimeException e) {
      reader.close();
      materialized.close();
      throw e;
    }

    return CloseableIterators.wrap(
        new RowIterator(reader, tableNames),
        () -> {
          reader.close();
          materialized.close();
        }
    );
  }

  @Override
  protected InputEntity source()
  {
    return source;
  }

  @Override
  protected List<InputRow> parseInputRows(Map<String, Object> intermediateRow) throws ParseException
  {
    return Collections.singletonList(MapInputRowParser.parse(inputRowSchema, intermediateRow));
  }

  @Override
  protected List<Map<String, Object>> toMap(Map<String, Object> intermediateRow)
  {
    return Collections.singletonList(intermediateRow);
  }

  private List<String> resolveTableNames(ITsFileReader reader) throws IOException
  {
    final List<TableSchema> allTables = reader.getAllTableSchema();
    if (allTables.isEmpty()) {
      throw InvalidInput.exception(
          "TsFile[%s] contains no table schema. Only table-model TsFiles are supported; tree-model files "
          + "are not.",
          source.getUri()
      );
    }
    if (tables == null || tables.isEmpty()) {
      return allTables.stream().map(TableSchema::getTableName).collect(Collectors.toList());
    }

    final Set<String> available =
        allTables.stream().map(schema -> schema.getTableName().toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
    for (String table : tables) {
      if (!available.contains(table.toLowerCase(Locale.ROOT))) {
        throw InvalidInput.exception(
            "Table[%s] does not exist in the file. Available tables: %s",
            table,
            allTables.stream().map(TableSchema::getTableName).collect(Collectors.toList())
        );
      }
    }
    return tables;
  }

  /**
   * Columns to read for one table. {@code queryColumns} are passed to the TsFile reader; only
   * {@code outputColumns} end up in the row. The two differ when a column is read purely to make the reader
   * produce rows.
   */
  private static class TableProjection
  {
    private final List<String> queryColumns;
    private final Set<String> outputColumns;

    TableProjection(List<String> queryColumns, Set<String> outputColumns)
    {
      this.queryColumns = queryColumns;
      this.outputColumns = outputColumns;
    }
  }

  private TableProjection selectColumns(TableSchema tableSchema)
  {
    if (columns != null && !columns.isEmpty()) {
      return new TableProjection(columns, new HashSet<>(columns));
    }

    final List<IMeasurementSchema> columnSchemas = tableSchema.getColumnSchemas();
    final List<ColumnCategory> columnCategories = tableSchema.getColumnTypes();
    final List<String> physicalColumns = new ArrayList<>();
    final Set<String> tagColumns = new HashSet<>();
    final Set<String> supportedColumns = new HashSet<>();
    for (int i = 0; i < columnSchemas.size(); i++) {
      final String columnName = columnSchemas.get(i).getMeasurementName();
      if (TsFileInputFormat.TIME_COLUMN_NAME.equalsIgnoreCase(columnName)) {
        continue;
      }
      physicalColumns.add(columnName);
      if (columnCategories.get(i) == ColumnCategory.TAG) {
        tagColumns.add(columnName);
      }
      if (isSupportedType(columnSchemas.get(i).getType())) {
        supportedColumns.add(columnName);
      }
    }

    // The ingestion schema's filter is authoritative whenever it carries information: it already lists the
    // timestamp column, the dimensions, the transforms' inputs and the aggregators' inputs. MSQ EXTERN always
    // passes ColumnsFilter.all(), which carries none of that; there the projection comes from the fixed
    // dimensions, i.e. the external table's column signature.
    final ColumnsFilter columnsFilter = inputRowSchema.getColumnsFilter();
    List<String> selected = null;
    if (columnsFilter != null && !columnsFilter.equals(ColumnsFilter.all())) {
      selected = physicalColumns.stream().filter(columnsFilter::apply).collect(Collectors.toList());
    } else {
      final DimensionsSpec dimensionsSpec = inputRowSchema.getDimensionsSpec();
      if (dimensionsSpec != null && dimensionsSpec.hasFixedDimensions()) {
        final Set<String> wanted = new HashSet<>(dimensionsSpec.getDimensionNames());
        final List<String> projected = physicalColumns.stream().filter(wanted::contains).collect(Collectors.toList());
        if (!projected.isEmpty()) {
          selected = projected;
        }
      }
    }
    if (selected == null) {
      // Nothing in the schema narrows the columns down: read everything that can be converted to a row value.
      selected = physicalColumns.stream().filter(supportedColumns::contains).collect(Collectors.toList());
    }

    // The TsFile table reader only produces blocks while it has a measurement column to read, so a projection of
    // tag columns alone would yield no rows at all. Read one supported measurement column as a row carrier in
    // that case, without exposing it in the row.
    final List<String> queryColumns = new ArrayList<>(selected);
    if (selected.stream().noneMatch(column -> !tagColumns.contains(column))) {
      physicalColumns.stream()
                     .filter(column -> !tagColumns.contains(column))
                     .filter(supportedColumns::contains)
                     .findFirst()
                     .ifPresent(queryColumns::add);
    }

    return new TableProjection(queryColumns, new HashSet<>(selected));
  }

  private static boolean isSupportedType(TSDataType dataType)
  {
    switch (dataType) {
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case TEXT:
      case STRING:
      case DATE:
      case TIMESTAMP:
      case BLOB:
        return true;
      default:
        return false;
    }
  }

  /**
   * Iterates over the rows of every selected table, keeping at most one open {@link ResultSet} at a time.
   */
  private class RowIterator implements Iterator<Map<String, Object>>
  {
    private final ITsFileReader reader;
    private final Iterator<String> tableIterator;

    @Nullable
    private ResultSet resultSet = null;
    private List<String> currentColumns = null;
    private List<TSDataType> currentTypes = null;
    private Set<String> currentOutputColumns = null;
    @Nullable
    private Map<String, Object> bufferedRow = null;

    RowIterator(ITsFileReader reader, List<String> tableNames)
    {
      this.reader = reader;
      this.tableIterator = tableNames.iterator();
    }

    @Override
    public boolean hasNext()
    {
      if (bufferedRow != null) {
        return true;
      }

      while (true) {
        if (resultSet != null) {
          try {
            if (resultSet.next()) {
              bufferedRow = readCurrentRow();
              return true;
            }
          }
          catch (IOException e) {
            throw new UncheckedIOException(e);
          }
          closeCurrentResultSet();
        }

        if (!tableIterator.hasNext()) {
          closeCurrentResultSet();
          return false;
        }
        openTable(tableIterator.next());
      }
    }

    @Override
    public Map<String, Object> next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final Map<String, Object> row = bufferedRow;
      bufferedRow = null;
      return row;
    }

    private void openTable(String tableName)
    {
      final long queryStartTime;
      final long queryEndTime;
      if (interval == null) {
        queryStartTime = Long.MIN_VALUE;
        queryEndTime = Long.MAX_VALUE;
      } else {
        if (interval.getEndMillis() <= interval.getStartMillis()) {
          // An empty interval matches no row, so there is nothing to read.
          return;
        }
        // TsFile folds both time bounds into the comparison, while a Druid interval is end-exclusive.
        queryStartTime = interval.getStartMillis();
        queryEndTime = interval.getEndMillis() - 1;
      }

      final TableSchema tableSchema;
      try {
        tableSchema = reader.getTableSchemas(tableName)
                            .orElseThrow(
                                () -> InvalidInput.exception(
                                    "Table[%s] does not exist in the file",
                                    tableName
                                )
                            );
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      final TableProjection projection = selectColumns(tableSchema);
      final List<String> selectedColumns = projection.queryColumns;
      final List<TSDataType> types = new ArrayList<>(selectedColumns.size());
      for (String column : selectedColumns) {
        final int columnIndex = tableSchema.findColumnIndex(column);
        if (columnIndex < 0) {
          throw InvalidInput.exception(
              "Column[%s] does not exist in table[%s]",
              column,
              tableName
          );
        }
        types.add(tableSchema.getColumnSchemas().get(columnIndex).getType());
      }

      try {
        resultSet = reader.query(tableName, selectedColumns, queryStartTime, queryEndTime);
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      catch (ReadProcessException | NoTableException | NoMeasurementException e) {
        throw new RuntimeException(e);
      }
      currentColumns = selectedColumns;
      currentTypes = types;
      currentOutputColumns = projection.outputColumns;
    }

    private void closeCurrentResultSet()
    {
      if (resultSet != null) {
        resultSet.close();
        resultSet = null;
      }
    }

    private Map<String, Object> readCurrentRow()
    {
      final Map<String, Object> row = new HashMap<>();

      for (int i = 0; i < currentColumns.size(); i++) {
        final String column = currentColumns.get(i);
        final int resultSetIndex = i + 2;
        if (currentOutputColumns.contains(column) && !resultSet.isNull(resultSetIndex)) {
          row.put(column, readValue(resultSet, currentTypes.get(i), resultSetIndex));
        }
      }

      // Written after the measurements so that a column sharing the timestamp column name is shadowed by it.
      // Column index 1 of every TsFile result set is the implicit time column.
      final long timestamp = resultSet.getLong(1);
      row.put(TsFileInputFormat.TIME_COLUMN_NAME, timestamp);

      final String timestampColumn = inputRowSchema.getTimestampSpec().getTimestampColumn();
      if (!TsFileInputFormat.TIME_COLUMN_NAME.equals(timestampColumn)) {
        row.put(timestampColumn, timestamp);
      }
      return row;
    }
  }

  private static Object readValue(ResultSet resultSet, TSDataType dataType, int columnIndex)
  {
    switch (dataType) {
      case INT32:
        return resultSet.getInt(columnIndex);
      case INT64:
        return resultSet.getLong(columnIndex);
      case FLOAT:
        return resultSet.getFloat(columnIndex);
      case DOUBLE:
        return resultSet.getDouble(columnIndex);
      case BOOLEAN:
        return resultSet.getBoolean(columnIndex);
      case TEXT:
      case STRING:
        return resultSet.getString(columnIndex);
      case DATE:
        return resultSet.getDate(columnIndex).toEpochDay();
      case TIMESTAMP:
        return resultSet.getLong(columnIndex);
      case BLOB:
        return Base64.getEncoder().encodeToString(resultSet.getBinary(columnIndex));
      default:
        throw InvalidInput.exception(
            "TsFile column[%s] has an unsupported data type[%s]",
            resultSet.getMetadata().getColumnName(columnIndex),
            dataType
        );
    }
  }
}
