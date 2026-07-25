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

package org.apache.druid.segment.incremental;

import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A clustered base table whose {@code region} clustering column is <em>derived</em> from a nested {@code extra_attrs}
 * column (absent from the raw row), carrying an aggregate projection that groups on that same {@code region}. Asserts
 * the projection groups on the derived value the clustering path produced, rather than a null that would collapse the
 * distinct (tenant, region) groups into one.
 */
class IncrementalIndexClusteredProjectionDerivedColumnTest extends InitializedNullHandlingTest
{
  private static final long T0 = DateTimes.of("2026-01-01T00:00:00").getMillis();
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec("ts", "millis", null);

  private static MapBasedInputRow row(long ts, String tenant, String region, long x)
  {
    final Map<String, Object> event = new LinkedHashMap<>();
    event.put("ts", ts);
    event.put("tenant", tenant);
    // `region` is not present as a top-level field; it is derived from the nested extra_attrs column.
    event.put("extra_attrs", Map.of("region", region));
    event.put("x", x);
    return new MapBasedInputRow(ts, List.of("tenant", "x"), event);
  }

  private static OnheapIncrementalIndex clusteredWithDerivedColumnProjection()
  {
    // Clustered by tenant + a `region` derived from the nested extra_attrs column, and carrying a projection that
    // groups on that derived `region`.
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .virtualColumns(VirtualColumns.create(
            new NestedFieldVirtualColumn("extra_attrs", "$.region", "region", ColumnType.STRING)
        ))
        .columns(
            new StringDimensionSchema("tenant"),
            new StringDimensionSchema("region"),   // clustering column derived from extra_attrs
            AutoTypeColumnSchema.of("extra_attrs"),
            new LongDimensionSchema("x"),
            new LongDimensionSchema("__time")
        )
        .clusteringColumns("tenant", "region")
        .build();

    final AggregateProjectionSpec projectionSpec =
        AggregateProjectionSpec.builder("proj")
                               .groupingColumns(
                                   new StringDimensionSchema("tenant"),
                                   new StringDimensionSchema("region")
                               )
                               .aggregators(new LongSumAggregatorFactory("sum_x", "x"))
                               .build();

    final IncrementalIndexSchema schema = IncrementalIndexSchema.builder()
        .withMinTimestamp(T0)
        .withTimestampSpec(TIMESTAMP_SPEC)
        .withQueryGranularity(Granularities.NONE)
        .withDimensionsSpec(spec.getDimensionsSpec())
        .withRollup(false)
        .withClusterSpec(spec)
        .withProjections(List.of(projectionSpec))
        .build();

    final OnheapIncrementalIndex index = (OnheapIncrementalIndex) new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(10_000)
        .build();
    index.add(row(T0, "acme", "us-east-1", 10));
    index.add(row(T0 + 1, "acme", "us-west-2", 5));
    index.add(row(T0 + 2, "globex", "eu-west-1", 7));
    return index;
  }

  @Test
  void testProjectionGroupsOnDerivedClusteringColumn()
  {
    try (OnheapIncrementalIndex index = clusteredWithDerivedColumnProjection()) {
      // The projection's `region` grouping column is derived from extra_attrs; it must carry the derived value the
      // clustering path produces, not null, so the three distinct (tenant, region) groups form.
      Assertions.assertEquals(
          Set.of(
              List.of("acme", "us-east-1"),
              List.of("acme", "us-west-2"),
              List.of("globex", "eu-west-1")
          ),
          ClusteredProjectionTestUtils.projectionGroupingTuples(index, "proj")
      );
    }
  }
}
