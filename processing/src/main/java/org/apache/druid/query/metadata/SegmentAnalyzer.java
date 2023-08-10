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
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.segment.index.semantic.DictionaryEncodedStringValueIndex;
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
    final int numRows = storageAdapter.getNumRows();

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

      if (capabilities == null) {
        log.warn("Unknown column type for column[%s]", columnName);
        columns.put(columnName, ColumnAnalysis.error("unknown_type"));
        continue;
      }

      ColumnAnalysis analysis;
      try {
        switch (capabilities.getType()) {
          case LONG:
            final int bytesPerRow =
                ColumnHolder.TIME_COLUMN_NAME.equals(columnName) ? NUM_BYTES_IN_TIMESTAMP : Long.BYTES;

            analysis = analyzeNumericColumn(capabilities, numRows, bytesPerRow);
            break;
          case FLOAT:
            analysis = analyzeNumericColumn(capabilities, numRows, NUM_BYTES_IN_TEXT_FLOAT);
            break;
          case DOUBLE:
            analysis = analyzeNumericColumn(capabilities, numRows, Double.BYTES);
            break;
          case STRING:
            if (index != null) {
              analysis = analyzeStringColumn(capabilities, index.getColumnHolder(columnName));
            } else {
              analysis = analyzeStringColumn(capabilities, storageAdapter, columnName);
            }
            break;
          case ARRAY:
            analysis = analyzeArrayColumn(capabilities);
            break;
          case COMPLEX:
            final ColumnHolder columnHolder = index != null ? index.getColumnHolder(columnName) : null;
            analysis = analyzeComplexColumn(capabilities, numRows, columnHolder);
            break;
          default:
            log.warn("Unknown column type[%s] for column[%s].", capabilities.asTypeString(), columnName);
            analysis = ColumnAnalysis.error(StringUtils.format("unknown_type_%s", capabilities.asTypeString()));
        }
      }
      catch (RuntimeException re) {
        // eat the exception and add error analysis, this is preferrable to exploding since exploding results in
        // the broker downstream SQL metadata cache left in a state where it is unable to completely finish
        // the SQL schema relies on this stuff functioning, and so will continuously retry when it faces a failure
        log.warn(re, "Error analyzing column[%s] of type[%s]", columnName, capabilities.asTypeString());
        analysis = ColumnAnalysis.error(re.getMessage());
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
    final ColumnAnalysis.Builder bob = ColumnAnalysis.builder().withCapabilities(capabilities);

    if (analyzingSize()) {
      if (capabilities.hasMultipleValues().isTrue()) {
        return bob.withErrorMessage("multi_value").build();
      }

      size = ((long) length) * sizePerRow;
    }
    return bob.withSize(size).build();
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
        if (column instanceof DictionaryEncodedColumn) {
          DictionaryEncodedColumn<String> theColumn = (DictionaryEncodedColumn<String>) column;
          cardinality = theColumn.getCardinality();
          if (analyzingMinMax() && cardinality > 0) {
            min = NullHandling.nullToEmptyIfNeeded(theColumn.lookupName(0));
            max = NullHandling.nullToEmptyIfNeeded(theColumn.lookupName(cardinality - 1));
          }
        } else {
          cardinality = 0;
        }
      }
      catch (IOException e) {
        return ColumnAnalysis.builder().withCapabilities(capabilities).withErrorMessage(e.getMessage()).build();
      }
    } else {
      cardinality = 0;
    }

    return ColumnAnalysis.builder()
                         .withCapabilities(capabilities)
                         .withSize(size)
                         .withCardinality(analyzingCardinality() ? cardinality : 0)
                         .withMinValue(min)
                         .withMaxValue(max)
                         .build();

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

    return ColumnAnalysis.builder()
                         .withCapabilities(capabilities)
                         .withSize(size)
                         .withCardinality(cardinality)
                         .withMinValue(min)
                         .withMaxValue(max)
                         .build();
  }

  private ColumnAnalysis analyzeComplexColumn(
      final ColumnCapabilities capabilities,
      final int numCells,
      @Nullable final ColumnHolder columnHolder
  )
  {
    final TypeSignature<ValueType> typeSignature = capabilities == null ? ColumnType.UNKNOWN_COMPLEX : capabilities;
    final String typeName = typeSignature.getComplexTypeName();

    final ColumnAnalysis.Builder bob = ColumnAnalysis.builder()
                                                     .withType(ColumnTypeFactory.ofType(typeSignature))
                                                     .withTypeName(typeName);

    try (final BaseColumn theColumn = columnHolder != null ? columnHolder.getColumn() : null) {
      if (capabilities != null) {
        bob.hasMultipleValues(capabilities.hasMultipleValues().isTrue())
           .hasNulls(capabilities.hasNulls().isMaybeTrue());
      }

      if (theColumn != null && !(theColumn instanceof ComplexColumn)) {
        return bob.withErrorMessage(
                    StringUtils.format(
                        "[%s] is not a [%s]",
                        theColumn.getClass().getName(),
                        ComplexColumn.class.getName()
                    )
                  )
                  .build();
      }
      final ComplexColumn complexColumn = (ComplexColumn) theColumn;

      long size = 0;
      if (analyzingSize() && complexColumn != null) {

        final ComplexMetricSerde serde = typeName == null ? null : ComplexMetrics.getSerdeForType(typeName);
        if (serde == null) {
          return bob.withErrorMessage(StringUtils.format("unknown_complex_%s", typeName)).build();
        }

        final Function<Object, Long> inputSizeFn = serde.inputSizeFn();
        if (inputSizeFn != null) {

          for (int i = 0; i < numCells; ++i) {
            size += inputSizeFn.apply(complexColumn.getRowValue(i));
          }
        }
      }
      return bob.withSize(size).build();
    }
    catch (IOException e) {
      return bob.withErrorMessage(e.getMessage()).build();
    }
  }

  private ColumnAnalysis analyzeArrayColumn(final ColumnCapabilities capabilities)
  {
    return ColumnAnalysis.builder().withCapabilities(capabilities).build();
  }
}
