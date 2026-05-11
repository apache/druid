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

import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.IndexSpec;
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
}
