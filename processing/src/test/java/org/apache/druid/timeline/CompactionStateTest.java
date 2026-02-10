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
import org.junit.Assert;
import org.junit.Test;

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
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(null);
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

    Assert.assertEquals(partitionsSpec, state.getPartitionsSpec());
    Assert.assertEquals(dimensionsSpec, state.getDimensionsSpec());
    Assert.assertEquals(metricsSpec, state.getMetricsSpec());
    Assert.assertEquals(transformSpec, state.getTransformSpec());
    Assert.assertEquals(indexSpec, state.getIndexSpec());
    Assert.assertEquals(granularitySpec, state.getGranularitySpec());
    Assert.assertNull(state.getProjections());
  }

  @Test
  public void testBuilderWithProjections()
  {
    DynamicPartitionsSpec partitionsSpec = new DynamicPartitionsSpec(null, null);
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder().build();
    List<AggregatorFactory> metricsSpec = List.of(new CountAggregatorFactory("count"));
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(null);
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

    Assert.assertEquals(projections, state.getProjections());
  }

  @Test
  public void testToBuilder()
  {
    DynamicPartitionsSpec partitionsSpec = new DynamicPartitionsSpec(null, null);
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder().build();
    List<AggregatorFactory> metricsSpec = List.of(new CountAggregatorFactory("count"));
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(null);
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

    Assert.assertEquals(originalState, rebuiltState);
    Assert.assertEquals(originalState.getPartitionsSpec(), rebuiltState.getPartitionsSpec());
    Assert.assertEquals(originalState.getDimensionsSpec(), rebuiltState.getDimensionsSpec());
    Assert.assertEquals(originalState.getMetricsSpec(), rebuiltState.getMetricsSpec());
    Assert.assertEquals(originalState.getTransformSpec(), rebuiltState.getTransformSpec());
    Assert.assertEquals(originalState.getIndexSpec(), rebuiltState.getIndexSpec());
    Assert.assertEquals(originalState.getGranularitySpec(), rebuiltState.getGranularitySpec());
    Assert.assertEquals(originalState.getProjections(), rebuiltState.getProjections());
  }

  @Test
  public void testToBuilderWithModifications()
  {
    DynamicPartitionsSpec partitionsSpec = new DynamicPartitionsSpec(null, null);
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder().build();
    List<AggregatorFactory> metricsSpec = List.of(new CountAggregatorFactory("count"));
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(null);
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

    Assert.assertNotEquals(originalState, modifiedState);
    Assert.assertEquals(originalState.getPartitionsSpec(), modifiedState.getPartitionsSpec());
    Assert.assertNotEquals(originalState.getDimensionsSpec(), modifiedState.getDimensionsSpec());
    Assert.assertEquals(newDimensionsSpec, modifiedState.getDimensionsSpec());
    Assert.assertEquals(originalState.getMetricsSpec(), modifiedState.getMetricsSpec());
  }

  @Test
  public void testBuilderIndependence()
  {
    DynamicPartitionsSpec partitionsSpec1 = new DynamicPartitionsSpec(100, null);
    DynamicPartitionsSpec partitionsSpec2 = new DynamicPartitionsSpec(200, null);
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder().build();
    List<AggregatorFactory> metricsSpec = List.of(new CountAggregatorFactory("count"));
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(null);
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
    Assert.assertEquals(partitionsSpec1, originalState.getPartitionsSpec());
    // Modified state should have the new partitionsSpec
    Assert.assertEquals(partitionsSpec2, modifiedState.getPartitionsSpec());
  }
}
