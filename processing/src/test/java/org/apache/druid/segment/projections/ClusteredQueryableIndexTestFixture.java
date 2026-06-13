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

package org.apache.druid.segment.projections;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test fixture that builds a real clustered base-table {@link QueryableIndex} — ingested into a clustered
 * {@link org.apache.druid.segment.incremental.OnheapIncrementalIndex}, persisted + merged through
 * {@link org.apache.druid.segment.IndexMergerV10}, and reloaded — so cursor-factory tests exercise the full
 * per-group {@link ClusteringColumnSelectorFactory} wrapping {@link org.apache.druid.segment.ConcatenatingCursor}
 * pipeline against actual on-disk clustered segment data.
 * <p>
 * The builder takes clustering tuples + per-group non-clustering rows; {@link Builder#build()} synthesizes full
 * input rows (clustering values merged into each row) and lets the writer partition them into cluster groups,
 * exactly as production ingestion does. (Earlier this fixture hand-wired fake per-group indexes behind a stub
 * {@code SimpleQueryableIndex}; now that the write side exists it builds genuine clustered segments.)
 * <p>
 * Holds {@link AutoCloseable} state (the mmap'd segment + a temp directory), so callers should manage it via
 * {@link Closeable#close()} or a {@code try-with-resources}.
 */
public final class ClusteredQueryableIndexTestFixture implements Closeable
{
  public static Builder builder()
  {
    return new Builder();
  }

  private final QueryableIndex segmentIndex;
  private final ClusteredValueGroupsBaseTableSchema summary;
  private final Closer closer;

  private ClusteredQueryableIndexTestFixture(
      QueryableIndex segmentIndex,
      ClusteredValueGroupsBaseTableSchema summary,
      Closer closer
  )
  {
    this.segmentIndex = segmentIndex;
    this.summary = summary;
    this.closer = closer;
  }

  /**
   * The clustered "segment" {@link QueryableIndex}; hand this to
   * {@link org.apache.druid.segment.QueryableIndexCursorFactory}.
   */
  public QueryableIndex segmentIndex()
  {
    return segmentIndex;
  }

  public ClusteredValueGroupsBaseTableSchema summary()
  {
    return summary;
  }

  public List<TableClusterGroupSpec> specs()
  {
    return summary.getClusterGroups();
  }

  @Override
  public void close()
  {
    try {
      closer.close();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static final class Builder
  {
    private Interval interval;
    private RowSignature clusteringColumns;
    private DimensionsSpec dimensionsSpec = DimensionsSpec.EMPTY;
    private AggregatorFactory[] metrics = new AggregatorFactory[]{new CountAggregatorFactory("count")};
    private boolean rollup = false;
    private final List<GroupDescriptor> groups = new ArrayList<>();

    private Builder()
    {
    }

    public Builder interval(Interval i)
    {
      this.interval = i;
      return this;
    }

    public Builder clusteringColumns(RowSignature sig)
    {
      this.clusteringColumns = sig;
      return this;
    }

    public Builder nonClusteringDimensions(DimensionSchema... dims)
    {
      this.dimensionsSpec = DimensionsSpec.builder().setDimensions(Arrays.asList(dims)).build();
      return this;
    }

    public Builder metrics(AggregatorFactory... m)
    {
      this.metrics = m;
      return this;
    }

    public Builder rollup(boolean r)
    {
      this.rollup = r;
      return this;
    }

    /**
     * Add one cluster group with its positional clustering tuple + per-group rows (non-clustering columns only).
     * The clustering values are merged into each row at build time so the writer routes them into the right group.
     */
    public Builder addGroup(List<?> clusteringValues, List<InputRow> rows)
    {
      groups.add(new GroupDescriptor(clusteringValues, rows));
      return this;
    }

    public ClusteredQueryableIndexTestFixture build()
    {
      if (interval == null) {
        throw new IllegalStateException("interval is required");
      }
      if (clusteringColumns == null || clusteringColumns.size() == 0) {
        throw new IllegalStateException("clusteringColumns is required");
      }
      if (groups.isEmpty()) {
        throw new IllegalStateException("at least one group is required");
      }
      for (int i = 0; i < groups.size(); i++) {
        if (groups.get(i).clusteringValues.size() != clusteringColumns.size()) {
          throw new IllegalStateException(
              "group " + i + " clusteringValues size [" + groups.get(i).clusteringValues.size()
              + "] does not match clusteringColumns size [" + clusteringColumns.size() + "]"
          );
        }
      }

      // Build the clustered base-table spec from the declared clustering signature + non-clustering dims/metrics.
      final List<DimensionSchema> clusteringDims = new ArrayList<>(clusteringColumns.size());
      for (int i = 0; i < clusteringColumns.size(); i++) {
        final String name = clusteringColumns.getColumnName(i);
        final ColumnType type = clusteringColumns.getColumnType(i).orElseThrow(
            () -> DruidException.defensive("clustering column has no type")
        );
        clusteringDims.add(dimensionSchemaFor(name, type));
      }
      final ClusteredValueGroupsBaseTableProjectionSpec clusterSpec =
          ClusteredValueGroupsBaseTableProjectionSpec.builder()
              .clusteringColumns(clusteringDims)
              .dimensions(dimensionsSpec.getDimensions())
              .metrics(metrics)
              .build();
      final IncrementalIndexSchema schema = IncrementalIndexSchema.builder()
          .withMinTimestamp(interval.getStartMillis())
          .withTimestampSpec(new TimestampSpec("__time", "auto", null))
          .withQueryGranularity(Granularities.NONE)
          .withDimensionsSpec(clusterSpec.getDimensionsSpec())
          .withMetrics(metrics)
          .withRollup(rollup)
          .withClusterSpec(clusterSpec)
          .build();

      // Synthesize full input rows: each group's clustering tuple merged into each of its non-clustering rows.
      final List<InputRow> allRows = new ArrayList<>();
      for (GroupDescriptor group : groups) {
        for (InputRow nonClusteringRow : group.rows) {
          allRows.add(mergeRow(group.clusteringValues, nonClusteringRow));
        }
      }

      final Closer closer = Closer.create();
      final File tmpDir = FileUtils.createTempDir("clusteredFixture");
      closer.register(() -> FileUtils.deleteDirectory(tmpDir));

      final QueryableIndex segmentIndex = IndexBuilder.create()
          .useV10()
          .tmpDir(tmpDir)
          .schema(schema)
          .rows(allRows)
          .buildMMappedIndex(interval);
      closer.register(segmentIndex);

      final ClusteredValueGroupsBaseTableSchema summary = segmentIndex.getClusteredBaseSummary();
      if (summary == null) {
        throw DruidException.defensive("expected a clustered segment but loaded summary was null");
      }
      return new ClusteredQueryableIndexTestFixture(segmentIndex, summary, closer);
    }

    private InputRow mergeRow(List<?> clusteringValues, InputRow nonClusteringRow)
    {
      final List<String> dims = new ArrayList<>(clusteringColumns.size() + dimensionsSpec.getDimensions().size());
      final Map<String, Object> event = new HashMap<>();
      for (int i = 0; i < clusteringColumns.size(); i++) {
        final String name = clusteringColumns.getColumnName(i);
        dims.add(name);
        event.put(name, clusteringValues.get(i));
      }
      for (DimensionSchema dim : dimensionsSpec.getDimensions()) {
        dims.add(dim.getName());
        event.put(dim.getName(), nonClusteringRow.getRaw(dim.getName()));
      }
      return new MapBasedInputRow(nonClusteringRow.getTimestamp(), dims, event);
    }

    private static DimensionSchema dimensionSchemaFor(String name, ColumnType type)
    {
      if (type.is(ValueType.STRING)) {
        return new StringDimensionSchema(name);
      }
      if (type.is(ValueType.LONG)) {
        return new LongDimensionSchema(name);
      }
      if (type.is(ValueType.DOUBLE)) {
        return new DoubleDimensionSchema(name);
      }
      if (type.is(ValueType.FLOAT)) {
        return new FloatDimensionSchema(name);
      }
      throw DruidException.defensive("unsupported clustering type [%s] for column [%s]", type, name);
    }
  }

  private record GroupDescriptor(List<?> clusteringValues, List<InputRow> rows) {}
}
