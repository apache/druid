/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.datasketches.quantiles;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.yahoo.sketches.quantiles.DoublesSketch;
import io.druid.initialization.DruidModule;
import io.druid.segment.serde.ComplexMetrics;

import java.util.Arrays;
import java.util.List;

public class QuantilesSketchModule implements DruidModule
{
  public static final String QUANTILES_SKETCH = "datasketchesQuantilesSketch";

  public static final String QUANTILES_SKETCH_MERGE_AGG = "datasketchesQuantilesSketchMerge";
  public static final String QUANTILES_SKETCH_BUILD_AGG = "datasketchesQuantilesSketchBuild";

  public static final String QUANTILES_POST_AGG = "datasketchesQuantiles";
  public static final String QUANTILE_POST_AGG = "datasketchesQuantile";
  public static final String CUSTOM_SPLITS_HISTOGRAM_POST_AGG = "datasketchesCustomSplitsHistogram";
  public static final String EQUAL_SPLITS_HISTOGRAM_POST_AGG = "datasketchesEqualSplitsHistogram";
  public static final String MIN_POST_AGG = "datasketchesQuantilesSketchMin";
  public static final String MAX_POST_AGG = "datasketchesQuantilesSketchMax";

  @Override
  public void configure(Binder binder)
  {
    if (ComplexMetrics.getSerdeForType(QUANTILES_SKETCH) == null) {
      ComplexMetrics.registerSerde(QUANTILES_SKETCH, new QuantilesSketchMergeComplexMetricSerde());
    }

    if (ComplexMetrics.getSerdeForType(QUANTILES_SKETCH_MERGE_AGG) == null) {
      ComplexMetrics.registerSerde(QUANTILES_SKETCH_MERGE_AGG, new QuantilesSketchMergeComplexMetricSerde());
    }

    if (ComplexMetrics.getSerdeForType(QUANTILES_SKETCH_BUILD_AGG) == null) {
      ComplexMetrics.registerSerde(QUANTILES_SKETCH_BUILD_AGG, new QuantilesSketchBuildComplexMetricSerde());
    }
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Arrays.<Module>asList(
        new SimpleModule("QuantilesSketchModule")
            .registerSubtypes(
                new NamedType(QuantilesSketchAggregatorFactory.class, QUANTILES_SKETCH),
                new NamedType(QuantilesPostAggregator.class, QUANTILES_POST_AGG),
                new NamedType(QuantilePostAggregator.class, QUANTILE_POST_AGG),
                new NamedType(CustomSplitsHistogramPostAggregator.class, CUSTOM_SPLITS_HISTOGRAM_POST_AGG),
                new NamedType(EqualSplitsHistogramPostAggregator.class, EQUAL_SPLITS_HISTOGRAM_POST_AGG),
                new NamedType(MinPostAggregator.class, MIN_POST_AGG),
                new NamedType(MaxPostAggregator.class, MAX_POST_AGG)
            )
            .addSerializer(
                DoublesSketch.class, new QuantilesSketchJsonSerializer()
            )
        //would need to handle Memory here once off-heap is supported.
    );
  }
}
