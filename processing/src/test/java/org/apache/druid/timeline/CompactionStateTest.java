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

package org.apache.druid.timeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.indexer.granularity.SegmentGranularitySpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class CompactionStateTest
{
  @Test
  public void testBuilderWithEmptyConstructor()
  {
    DynamicPartitionsSpec partitionsSpec = new DynamicPartitionsSpec(null, null);
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder().build();
    List<AggregatorFactory> metricsSpec = List.of(new CountAggregatorFactory("count"));
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(null, null);
    IndexSpec indexSpec = IndexSpec.getDefault();
    UniformGranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.NONE,
        Collections.emptyList()
    );

    CompactionState state = new CompactionState.Builder()
        .partitionsSpec(partitionsSpec)
        .dimensionsSpec(dimensionsSpec)
        .metricsSpec(metricsSpec)
        .transformSpec(transformSpec)
        .indexSpec(indexSpec)
        .granularitySpec(granularitySpec)
        .build();

    Assertions.assertEquals(partitionsSpec, state.getPartitionsSpec());
    Assertions.assertEquals(dimensionsSpec, state.getDimensionsSpec());
    Assertions.assertEquals(metricsSpec, state.getMetricsSpec());
    Assertions.assertEquals(transformSpec, state.getTransformSpec());
    Assertions.assertEquals(indexSpec, state.getIndexSpec());
    Assertions.assertEquals(granularitySpec, state.getGranularitySpec());
    Assertions.assertNull(state.getProjections());
  }

  @Test
  public void testBuilderWithProjections()
  {
    DynamicPartitionsSpec partitionsSpec = new DynamicPartitionsSpec(null, null);
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder().build();
    List<AggregatorFactory> metricsSpec = List.of(new CountAggregatorFactory("count"));
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(null, null);
    IndexSpec indexSpec = IndexSpec.getDefault();
    UniformGranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.NONE,
        Collections.emptyList()
    );
    List<AggregateProjectionSpec> projections = List.of(
        new AggregateProjectionSpec(
            "proj1",
            null,
            VirtualColumns.EMPTY,
            Collections.emptyList(),
            new AggregatorFactory[]{new CountAggregatorFactory("count")}
        )
    );

    CompactionState state = new CompactionState.Builder()
        .partitionsSpec(partitionsSpec)
        .dimensionsSpec(dimensionsSpec)
        .metricsSpec(metricsSpec)
        .transformSpec(transformSpec)
        .indexSpec(indexSpec)
        .granularitySpec(granularitySpec)
        .projections(projections)
        .build();

    Assertions.assertEquals(projections, state.getProjections());
  }

  @Test
  public void testBuilderWithBaseTable()
  {
    DynamicPartitionsSpec partitionsSpec = new DynamicPartitionsSpec(null, null);
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder().build();
    List<AggregatorFactory> metricsSpec = List.of(new CountAggregatorFactory("count"));
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(null, null);
    IndexSpec indexSpec = IndexSpec.getDefault();
    UniformGranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.NONE,
        Collections.emptyList()
    );
    ClusteredValueGroupsBaseTableProjectionSpec baseTable =
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
                                                   .columns(
                                                       new StringDimensionSchema("tenant"),
                                                       new LongDimensionSchema("__time")
                                                   )
                                                   .clusteringColumns("tenant")
                                                   .build();

    CompactionState state = new CompactionState.Builder()
        .partitionsSpec(partitionsSpec)
        .dimensionsSpec(dimensionsSpec)
        .metricsSpec(metricsSpec)
        .transformSpec(transformSpec)
        .indexSpec(indexSpec)
        .granularitySpec(granularitySpec)
        .baseTable(baseTable)
        .build();

    Assertions.assertEquals(baseTable, state.getBaseTable());

    // round-trips through the builder
    Assertions.assertEquals(baseTable, state.toBuilder().build().getBaseTable());
  }

  @Test
  public void testToBuilder()
  {
    DynamicPartitionsSpec partitionsSpec = new DynamicPartitionsSpec(null, null);
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder().build();
    List<AggregatorFactory> metricsSpec = List.of(new CountAggregatorFactory("count"));
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(null, null);
    IndexSpec indexSpec = IndexSpec.getDefault();
    UniformGranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.NONE,
        Collections.emptyList()
    );
    List<AggregateProjectionSpec> projections = List.of(
        new AggregateProjectionSpec(
            "proj1",
            null,
            VirtualColumns.EMPTY,
            Collections.emptyList(),
            new AggregatorFactory[]{new CountAggregatorFactory("count")}
        )
    );

    CompactionState originalState = new CompactionState(
        partitionsSpec,
        dimensionsSpec,
        metricsSpec,
        transformSpec,
        indexSpec,
        granularitySpec,
        null,
        null,
        projections
    );

    CompactionState rebuiltState = originalState.toBuilder().build();

    Assertions.assertEquals(originalState, rebuiltState);
    Assertions.assertEquals(originalState.getPartitionsSpec(), rebuiltState.getPartitionsSpec());
    Assertions.assertEquals(originalState.getDimensionsSpec(), rebuiltState.getDimensionsSpec());
    Assertions.assertEquals(originalState.getMetricsSpec(), rebuiltState.getMetricsSpec());
    Assertions.assertEquals(originalState.getTransformSpec(), rebuiltState.getTransformSpec());
    Assertions.assertEquals(originalState.getIndexSpec(), rebuiltState.getIndexSpec());
    Assertions.assertEquals(originalState.getGranularitySpec(), rebuiltState.getGranularitySpec());
    Assertions.assertEquals(originalState.getProjections(), rebuiltState.getProjections());
  }

  @Test
  public void testToBuilderWithModifications()
  {
    DynamicPartitionsSpec partitionsSpec = new DynamicPartitionsSpec(null, null);
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder().build();
    List<AggregatorFactory> metricsSpec = List.of(new CountAggregatorFactory("count"));
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(null, null);
    IndexSpec indexSpec = IndexSpec.getDefault();
    UniformGranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.NONE,
        Collections.emptyList()
    );

    CompactionState originalState = new CompactionState(
        partitionsSpec,
        dimensionsSpec,
        metricsSpec,
        transformSpec,
        indexSpec,
        granularitySpec,
        null,
        null,
        null
    );

    // Modify dimensionsSpec using toBuilder
    DimensionsSpec newDimensionsSpec = DimensionsSpec.builder()
        .setDefaultSchemaDimensions(List.of("dim1", "dim2"))
        .build();

    CompactionState modifiedState = originalState.toBuilder()
        .dimensionsSpec(newDimensionsSpec)
        .build();

    Assertions.assertNotEquals(originalState, modifiedState);
    Assertions.assertEquals(originalState.getPartitionsSpec(), modifiedState.getPartitionsSpec());
    Assertions.assertNotEquals(originalState.getDimensionsSpec(), modifiedState.getDimensionsSpec());
    Assertions.assertEquals(newDimensionsSpec, modifiedState.getDimensionsSpec());
    Assertions.assertEquals(originalState.getMetricsSpec(), modifiedState.getMetricsSpec());
  }

  @Test
  public void testBuilderIndependence()
  {
    DynamicPartitionsSpec partitionsSpec1 = new DynamicPartitionsSpec(100, null);
    DynamicPartitionsSpec partitionsSpec2 = new DynamicPartitionsSpec(200, null);
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder().build();
    List<AggregatorFactory> metricsSpec = List.of(new CountAggregatorFactory("count"));
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(null, null);
    IndexSpec indexSpec = IndexSpec.getDefault();
    UniformGranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.NONE,
        Collections.emptyList()
    );

    CompactionState originalState = new CompactionState(
        partitionsSpec1,
        dimensionsSpec,
        metricsSpec,
        transformSpec,
        indexSpec,
        granularitySpec,
        null,
        null,
        null
    );

    CompactionState.Builder builder = originalState.toBuilder();
    builder.partitionsSpec(partitionsSpec2);

    CompactionState modifiedState = builder.build();

    // Original state should be unchanged
    Assertions.assertEquals(partitionsSpec1, originalState.getPartitionsSpec());
    // Modified state should have the new partitionsSpec
    Assertions.assertEquals(partitionsSpec2, modifiedState.getPartitionsSpec());
  }

  @Test
  public void testSerdeWithBaseTable() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    ClusteredValueGroupsBaseTableProjectionSpec baseTable =
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
                                                   .columns(
                                                       new StringDimensionSchema("tenant"),
                                                       new LongDimensionSchema("__time")
                                                   )
                                                   .clusteringColumns("tenant")
                                                   .build();

    CompactionState state = new CompactionState(
        new DynamicPartitionsSpec(null, null),
        DimensionsSpec.builder().build(),
        List.of(new CountAggregatorFactory("count")),
        new CompactionTransformSpec(null, null),
        IndexSpec.getDefault(),
        new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, Collections.emptyList()),
        null,
        baseTable,
        null
    );

    CompactionState roundTrip = mapper.readValue(mapper.writeValueAsString(state), CompactionState.class);
    Assertions.assertEquals(state, roundTrip);
    Assertions.assertEquals(baseTable, roundTrip.getBaseTable());
  }

  @Test
  public void testSerdeWithSegmentGranularitySpec() throws Exception
  {
    // The baseTable carries a query-granularity expression virtual column, whose deserialization needs an injected
    // ExprMacroTable, so use the TestHelper mapper instead of the plain DefaultObjectMapper.
    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    ClusteredValueGroupsBaseTableProjectionSpec baseTable =
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
                                                   .columns(
                                                       new StringDimensionSchema("tenant"),
                                                       new LongDimensionSchema("__time")
                                                   )
                                                   .clusteringColumns("tenant")
                                                   .build()
                                                   .withQueryGranularity(Granularities.HOUR);

    SegmentGranularitySpec segmentGranularitySpec = new SegmentGranularitySpec(Granularities.DAY, null);

    CompactionState state = new CompactionState(
        new DynamicPartitionsSpec(null, null),
        DimensionsSpec.builder().build(),
        List.of(new CountAggregatorFactory("count")),
        new CompactionTransformSpec(null, null),
        IndexSpec.getDefault(),
        null,
        segmentGranularitySpec,
        baseTable,
        null
    );

    final String json = mapper.writeValueAsString(state);
    CompactionState roundTrip = mapper.readValue(json, CompactionState.class);
    Assertions.assertEquals(state, roundTrip);
    Assertions.assertEquals(segmentGranularitySpec, roundTrip.getSegmentGranularitySpec());
    Assertions.assertEquals(baseTable, roundTrip.getBaseTable());
    Assertions.assertTrue(json.contains("segmentGranularitySpec"));
  }

  /**
   * Legacy (non-baseTable) CompactionState must serialize WITHOUT a "segmentGranularitySpec" key (the field is
   * {@code @JsonInclude(NON_NULL)}), so legacy fingerprints are byte-for-byte unchanged.
   */
  @Test
  public void testSerdeLegacyOmitsSegmentGranularitySpec() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    CompactionState state = new CompactionState(
        new DynamicPartitionsSpec(null, null),
        DimensionsSpec.builder().build(),
        List.of(new CountAggregatorFactory("count")),
        new CompactionTransformSpec(null, null),
        IndexSpec.getDefault(),
        new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, Collections.emptyList()),
        null,
        null,
        null
    );

    final String json = mapper.writeValueAsString(state);
    Assertions.assertFalse(json.contains("segmentGranularitySpec"));

    CompactionState roundTrip = mapper.readValue(json, CompactionState.class);
    Assertions.assertEquals(state, roundTrip);
    Assertions.assertNull(roundTrip.getSegmentGranularitySpec());
  }
}
