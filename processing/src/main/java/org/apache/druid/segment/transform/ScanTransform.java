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

package org.apache.druid.segment.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A multi-row transform that processes each input row through the scan query engine during ingestion.
 * Each input row is wrapped in a single-row segment and run through the configured {@link ScanQuery},
 * which can include UNNEST (via {@link org.apache.druid.query.UnnestDataSource}), filters, virtual columns, etc.
 *
 * If the query produces no output rows (e.g., empty/missing array), the input row passes through
 * with null values for any new columns.
 */
public class ScanTransform implements Transform
{
  private static final ScanQueryEngine ENGINE = new ScanQueryEngine();

  private final String name;
  private final ScanQuery query;

  @JsonCreator
  public ScanTransform(
      @JsonProperty("name") final String name,
      @JsonProperty("query") final ScanQuery query
  )
  {
    this.name = name;
    this.query = query;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  @Nullable
  public RowFunction getRowFunction()
  {
    return null;
  }

  @JsonProperty
  public ScanQuery getQuery()
  {
    return query;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return Set.copyOf(query.getDataSource().getTableNames());
  }

  @Override
  public boolean isMultiRow()
  {
    return true;
  }

  @Override
  public List<InputRow> applyMultiRow(final InputRow inputRow)
  {
    final RowSignature inputSignature = buildSignature(inputRow);

    final RowBasedSegment<InputRow> segment = new RowBasedSegment<>(
        Sequences.simple(List.of(inputRow)),
        RowAdapters.standardRow(),
        inputSignature
    );

    final Segment mappedSegment = applySegmentMapFunction(segment);

    final ScanQuery queryWithoutTimeout = query.withOverriddenContext(
        Map.of(QueryContexts.TIMEOUT_KEY, 0)
    );

    final List<ScanResultValue> scanResults = ENGINE.process(
        queryWithoutTimeout,
        mappedSegment,
        ResponseContext.createEmpty(),
        null
    ).toList();

    final List<InputRow> result = new ArrayList<>();

    for (final ScanResultValue scanResult : scanResults) {
      final List<String> dimensionColumns = resolveDimensionColumns(inputRow, scanResult.getColumns());
      @SuppressWarnings("unchecked")
      final List<Map<String, Object>> events = (List<Map<String, Object>>) scanResult.getEvents();
      for (final Map<String, Object> event : events) {
        result.add(new MapBasedInputRow(inputRow.getTimestampFromEpoch(), dimensionColumns, event));
      }
    }

    if (result.isEmpty()) {
      final List<String> dimensionColumns = resolveDimensionColumns(inputRow, null);
      final Map<String, Object> passthroughEvent = new LinkedHashMap<>();
      for (final String dim : inputRow.getDimensions()) {
        passthroughEvent.put(dim, inputRow.getRaw(dim));
      }
      result.add(new MapBasedInputRow(inputRow.getTimestampFromEpoch(), dimensionColumns, passthroughEvent));
    }

    return result;
  }

  private Segment applySegmentMapFunction(final Segment segment)
  {
    final SegmentMapFunction mapFunction = query.getDataSource().createSegmentMapFunction(query);
    final Optional<Segment> mapped = mapFunction.apply(Optional.of(segment));
    return mapped.orElse(segment);
  }

  private static RowSignature buildSignature(final InputRow inputRow)
  {
    final RowSignature.Builder signatureBuilder = RowSignature.builder();
    signatureBuilder.add(ColumnHolder.TIME_COLUMN_NAME, ColumnType.LONG);
    for (final String dim : inputRow.getDimensions()) {
      signatureBuilder.add(dim, ColumnType.NESTED_DATA);
    }
    return signatureBuilder.build();
  }

  private List<String> resolveDimensionColumns(final InputRow inputRow, @Nullable final List<String> scanResultColumns)
  {
    final LinkedHashSet<String> dims = new LinkedHashSet<>(inputRow.getDimensions());

    if (scanResultColumns != null) {
      for (final String col : scanResultColumns) {
        if (!ColumnHolder.TIME_COLUMN_NAME.equals(col)) {
          dims.add(col);
        }
      }
    }

    final List<String> queryColumns = query.getColumns();
    if (queryColumns != null) {
      for (final String col : queryColumns) {
        if (!ColumnHolder.TIME_COLUMN_NAME.equals(col)) {
          dims.add(col);
        }
      }
    }

    return new ArrayList<>(dims);
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ScanTransform that = (ScanTransform) o;
    return Objects.equals(name, that.name)
           && Objects.equals(query, that.query);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, query);
  }

  @Override
  public String toString()
  {
    return "ScanTransform{" +
           "name=" + name +
           ", query=" + query +
           '}';
  }
}
