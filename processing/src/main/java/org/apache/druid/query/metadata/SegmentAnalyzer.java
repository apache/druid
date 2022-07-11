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

package org.apache.druid.query.metadata;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ColumnTypeFactory;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.EnumSet;
import java.util.LinkedHashMap;
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

  private final EnumSet<SegmentMetadataQuery.AnalysisType> analysisTypes;

  public SegmentAnalyzer(EnumSet<SegmentMetadataQuery.AnalysisType> analysisTypes)
  {
    this.analysisTypes = analysisTypes;
  }

  public long numRows(Segment segment)
  {
    return Preconditions.checkNotNull(segment, "segment").asStorageAdapter().getNumRows();
  }

  public Map<String, ColumnAnalysis> analyze(Segment segment)
  {
    Preconditions.checkNotNull(segment, "segment");

    // index is null for incremental-index-based segments, but storageAdapter is always available
    final QueryableIndex index = segment.asQueryableIndex();
    final StorageAdapter storageAdapter = segment.asStorageAdapter();

    // get length and column names from storageAdapter
    final int length = storageAdapter.getNumRows();

    // Use LinkedHashMap to preserve column order.
    final Map<String, ColumnAnalysis> columns = new LinkedHashMap<>();

    final RowSignature rowSignature = storageAdapter.getRowSignature();
    for (String columnName : rowSignature.getColumnNames()) {
      final ColumnCapabilities capabilities;

      if (storageAdapter instanceof IncrementalIndexStorageAdapter) {
        // See javadocs for getSnapshotColumnCapabilities for a discussion of why we need to do this.
        capabilities = ((IncrementalIndexStorageAdapter) storageAdapter).getSnapshotColumnCapabilities(columnName);
      } else {
        capabilities = storageAdapter.getColumnCapabilities(columnName);
      }

      final ColumnAnalysis analysis;

      switch (capabilities.getType()) {
        case LONG:
          final int bytesPerRow =
              ColumnHolder.TIME_COLUMN_NAME.equals(columnName) ? NUM_BYTES_IN_TIMESTAMP : Long.BYTES;

          analysis = analyzeNumericColumn(capabilities, length, bytesPerRow);
          break;
        case FLOAT:
          analysis = analyzeNumericColumn(capabilities, length, NUM_BYTES_IN_TEXT_FLOAT);
          break;
        case DOUBLE:
          analysis = analyzeNumericColumn(capabilities, length, Double.BYTES);
          break;
        case STRING:
          if (index != null) {
            analysis = analyzeStringColumn(capabilities, index.getColumnHolder(columnName));
          } else {
            analysis = analyzeStringColumn(capabilities, storageAdapter, columnName);
          }
          break;
        case COMPLEX:
          final ColumnHolder columnHolder = index != null ? index.getColumnHolder(columnName) : null;
          analysis = analyzeComplexColumn(capabilities, columnHolder);
          break;
        default:
          log.warn("Unknown column type[%s].", capabilities.asTypeString());
          analysis = ColumnAnalysis.error(StringUtils.format("unknown_type_%s", capabilities.asTypeString()));
      }

      columns.put(columnName, analysis);
    }

    return columns;
  }

  public boolean analyzingSize()
  {
    return analysisTypes.contains(SegmentMetadataQuery.AnalysisType.SIZE);
  }

  public boolean analyzingCardinality()
  {
    return analysisTypes.contains(SegmentMetadataQuery.AnalysisType.CARDINALITY);
  }

  public boolean analyzingMinMax()
  {
    return analysisTypes.contains(SegmentMetadataQuery.AnalysisType.MINMAX);
  }

  private ColumnAnalysis analyzeNumericColumn(
      final ColumnCapabilities capabilities,
      final int length,
      final int sizePerRow
  )
  {
    long size = 0;

    if (analyzingSize()) {
      if (capabilities.hasMultipleValues().isTrue()) {
        return ColumnAnalysis.error("multi_value");
      }

      size = ((long) length) * sizePerRow;
    }

    return new ColumnAnalysis(
        capabilities.toColumnType(),
        capabilities.getType().name(),
        capabilities.hasMultipleValues().isTrue(),
        capabilities.hasNulls().isMaybeTrue(), // if we don't know for sure, then we should plan to check for nulls
        size,
        null,
        null,
        null,
        null
    );
  }

  private ColumnAnalysis analyzeStringColumn(
      final ColumnCapabilities capabilities,
      final ColumnHolder columnHolder
  )
  {
    Comparable min = null;
    Comparable max = null;
    long size = 0;
    final int cardinality;
    final ColumnIndexSupplier indexSupplier = columnHolder.getIndexSupplier();
    final DictionaryEncodedStringValueIndex valueIndex =
        indexSupplier == null ? null : indexSupplier.as(DictionaryEncodedStringValueIndex.class);
    if (valueIndex != null) {
      cardinality = valueIndex.getCardinality();
      if (analyzingSize()) {
        for (int i = 0; i < cardinality; ++i) {
          String value = valueIndex.getValue(i);
          if (value != null) {
            size += StringUtils.estimatedBinaryLengthAsUTF8(value) * ((long) valueIndex.getBitmap(i).size());
          }
        }
      }
      if (analyzingMinMax() && cardinality > 0) {
        min = NullHandling.nullToEmptyIfNeeded(valueIndex.getValue(0));
        max = NullHandling.nullToEmptyIfNeeded(valueIndex.getValue(cardinality - 1));
      }
    } else if (capabilities.isDictionaryEncoded().isTrue()) {
      // fallback if no bitmap index
      try (BaseColumn column = columnHolder.getColumn()) {
        DictionaryEncodedColumn<String> theColumn = (DictionaryEncodedColumn<String>) column;
        cardinality = theColumn.getCardinality();
        if (analyzingMinMax() && cardinality > 0) {
          min = NullHandling.nullToEmptyIfNeeded(theColumn.lookupName(0));
          max = NullHandling.nullToEmptyIfNeeded(theColumn.lookupName(cardinality - 1));
        }
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      cardinality = 0;
    }

    return new ColumnAnalysis(
        capabilities.toColumnType(),
        capabilities.getType().name(),
        capabilities.hasMultipleValues().isTrue(),
        capabilities.hasNulls().isMaybeTrue(), // if we don't know for sure, then we should plan to check for nulls
        size,
        analyzingCardinality() ? cardinality : 0,
        min,
        max,
        null
    );
  }

  private ColumnAnalysis analyzeStringColumn(
      final ColumnCapabilities capabilities,
      final StorageAdapter storageAdapter,
      final String columnName
  )
  {
    int cardinality = 0;
    long size = 0;

    Comparable min = null;
    Comparable max = null;

    if (analyzingCardinality()) {
      cardinality = storageAdapter.getDimensionCardinality(columnName);
    }

    if (analyzingSize()) {
      final DateTime start = storageAdapter.getMinTime();
      final DateTime end = storageAdapter.getMaxTime();

      final Sequence<Cursor> cursors =
          storageAdapter.makeCursors(
              null,
              new Interval(start, end),
              VirtualColumns.EMPTY,
              Granularities.ALL,
              false,
              null
          );

      size = cursors.accumulate(
          0L,
          new Accumulator<Long, Cursor>()
          {
            @Override
            public Long accumulate(Long accumulated, Cursor cursor)
            {
              DimensionSelector selector = cursor
                  .getColumnSelectorFactory()
                  .makeDimensionSelector(new DefaultDimensionSpec(columnName, columnName));
              if (selector == null) {
                return accumulated;
              }
              long current = accumulated;
              while (!cursor.isDone()) {
                final IndexedInts row = selector.getRow();
                for (int i = 0, rowSize = row.size(); i < rowSize; ++i) {
                  final String dimVal = selector.lookupName(row.get(i));
                  if (dimVal != null && !dimVal.isEmpty()) {
                    current += StringUtils.estimatedBinaryLengthAsUTF8(dimVal);
                  }
                }
                cursor.advance();
              }

              return current;
            }
          }
      );
    }

    if (analyzingMinMax()) {
      min = storageAdapter.getMinValue(columnName);
      max = storageAdapter.getMaxValue(columnName);
    }

    return new ColumnAnalysis(
        capabilities.toColumnType(),
        capabilities.getType().name(),
        capabilities.hasMultipleValues().isTrue(),
        capabilities.hasNulls().isMaybeTrue(), // if we don't know for sure, then we should plan to check for nulls
        size,
        cardinality,
        min,
        max,
        null
    );
  }

  private ColumnAnalysis analyzeComplexColumn(
      @Nullable final ColumnCapabilities capabilities,
      @Nullable final ColumnHolder columnHolder
  )
  {
    final TypeSignature<ValueType> typeSignature = capabilities == null ? ColumnType.UNKNOWN_COMPLEX : capabilities;
    final String typeName = typeSignature.getComplexTypeName();

    try (final ComplexColumn complexColumn = columnHolder != null ? (ComplexColumn) columnHolder.getColumn() : null) {
      final boolean hasMultipleValues = capabilities != null && capabilities.hasMultipleValues().isTrue();
      final boolean hasNulls = capabilities != null && capabilities.hasNulls().isMaybeTrue();
      long size = 0;

      if (analyzingSize() && complexColumn != null) {
        final ComplexMetricSerde serde = typeName == null ? null : ComplexMetrics.getSerdeForType(typeName);
        if (serde == null) {
          return ColumnAnalysis.error(StringUtils.format("unknown_complex_%s", typeName));
        }

        final Function<Object, Long> inputSizeFn = serde.inputSizeFn();
        if (inputSizeFn == null) {
          return new ColumnAnalysis(
              ColumnTypeFactory.ofType(typeSignature),
              typeName,
              hasMultipleValues,
              hasNulls,
              0,
              null,
              null,
              null,
              null
          );
        }

        final int length = complexColumn.getLength();
        for (int i = 0; i < length; ++i) {
          size += inputSizeFn.apply(complexColumn.getRowValue(i));
        }
      }

      return new ColumnAnalysis(
          ColumnTypeFactory.ofType(typeSignature),
          typeName,
          hasMultipleValues,
          hasNulls,
          size,
          null,
          null,
          null,
          null
      );
    }
  }
}
