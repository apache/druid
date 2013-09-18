/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.metadata;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import com.metamx.common.logger.Logger;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;

import java.util.Map;

public class SegmentAnalyzer
{
  private static final Logger log = new Logger(SegmentAnalyzer.class);

  /**
   * This is based on the minimum size of a timestamp (POSIX seconds).  An ISO timestamp will actually be more like 24+
   */
  private static final int NUM_BYTES_IN_TIMESTAMP = 10;

  /**
   * This is based on assuming 6 units of precision, one decimal point and a single value left of the decimal
   */
  private static final int NUM_BYTES_IN_TEXT_FLOAT = 8;

  public Map<String, ColumnAnalysis> analyze(QueryableIndex index)
  {
    Preconditions.checkNotNull(index, "Index cannot be null");

    Map<String, ColumnAnalysis> columns = Maps.newTreeMap();

    for (String columnName : index.getColumnNames()) {
      final Column column = index.getColumn(columnName);
      final ColumnCapabilities capabilities = column.getCapabilities();

      final ColumnAnalysis analysis;
      final ValueType type = capabilities.getType();
      switch(type) {
        case LONG:
          analysis = analyzeLongColumn(column);
          break;
        case FLOAT:
          analysis = analyzeFloatColumn(column);
          break;
        case STRING:
          analysis = analyzeStringColumn(column);
          break;
        case COMPLEX:
          analysis = analyzeComplexColumn(column);
          break;
        default:
          log.warn("Unknown column type[%s].", type);
          analysis = ColumnAnalysis.error(String.format("unknown_type_%s", type));
      }

      columns.put(columnName, analysis);
    }

    columns.put("__time", lengthBasedAnalysis(index.getTimeColumn(), NUM_BYTES_IN_TIMESTAMP));

    return columns;
  }

  public ColumnAnalysis analyzeLongColumn(Column column)
  {
    return lengthBasedAnalysis(column, Longs.BYTES);
  }

  public ColumnAnalysis analyzeFloatColumn(Column column)
  {
    return lengthBasedAnalysis(column, NUM_BYTES_IN_TEXT_FLOAT);
  }

  private ColumnAnalysis lengthBasedAnalysis(Column column, final int numBytes)
  {
    final ColumnCapabilities capabilities = column.getCapabilities();
    if (capabilities.hasMultipleValues()) {
      return ColumnAnalysis.error("multi_value");
    }

    return new ColumnAnalysis(capabilities.getType().name(), column.getLength() * numBytes, null, null);
  }

  public ColumnAnalysis analyzeStringColumn(Column column)
  {
    final ColumnCapabilities capabilities = column.getCapabilities();

    if (capabilities.hasBitmapIndexes()) {
      final BitmapIndex bitmapIndex = column.getBitmapIndex();

      int cardinality = bitmapIndex.getCardinality();
      long size = 0;
      for (int i = 0; i < cardinality; ++i) {
        String value = bitmapIndex.getValue(i);

        if (value != null) {
          size += value.getBytes(Charsets.UTF_8).length * bitmapIndex.getConciseSet(value).size();
        }
      }

      return new ColumnAnalysis(capabilities.getType().name(), size, cardinality, null);
    }

    return ColumnAnalysis.error("string_no_bitmap");
  }

  public ColumnAnalysis analyzeComplexColumn(Column column)
  {
    final ColumnCapabilities capabilities = column.getCapabilities();
    final ComplexColumn complexColumn = column.getComplexColumn();

    final String typeName = complexColumn.getTypeName();
    final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);
    if (serde == null) {
      return ColumnAnalysis.error(String.format("unknown_complex_%s", typeName));
    }

    final Function<Object, Long> inputSizeFn = serde.inputSizeFn();
    if (inputSizeFn == null) {
      return ColumnAnalysis.error("noSizeFn");
    }

    final int length = column.getLength();
    long size = 0;
    for (int i = 0; i < length; ++i) {
      size += inputSizeFn.apply(complexColumn.getRowValue(i));
    }

    return new ColumnAnalysis(capabilities.getType().name(), size, null, null);
  }
}
