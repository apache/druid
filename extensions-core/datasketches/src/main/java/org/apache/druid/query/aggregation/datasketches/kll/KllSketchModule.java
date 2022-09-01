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

package org.apache.druid.query.aggregation.datasketches.kll;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.serde.ComplexMetrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class KllSketchModule implements DruidModule
{
  public static final String DOUBLES_SKETCH = "KllDoublesSketch";
  public static final String DOUBLES_SKETCH_MERGE = "KllDoublesSketchMerge";
  public static final ColumnType DOUBLES_TYPE = ColumnType.ofComplex(DOUBLES_SKETCH);
  public static final ColumnType DOUBLES_MERGE_TYPE = ColumnType.ofComplex(DOUBLES_SKETCH_MERGE);

  public static final String DOUBLES_SKETCH_HISTOGRAM_POST_AGG = "KllDoublesSketchToHistogram";
  public static final String DOUBLES_SKETCH_QUANTILE_POST_AGG = "KllDoublesSketchToQuantile";
  public static final String DOUBLES_SKETCH_QUANTILES_POST_AGG = "KllDoublesSketchToQuantiles";
  public static final String DOUBLES_SKETCH_RANK_POST_AGG = "KllDoublesSketchToRank";
  public static final String DOUBLES_SKETCH_CDF_POST_AGG = "KllDoublesSketchToCDF";
  public static final String DOUBLES_SKETCH_TO_STRING_POST_AGG = "KllDoublesSketchToString";

  public static final String FLOATS_SKETCH = "KllFloatsSketch";
  public static final String FLOATS_SKETCH_MERGE = "KllFloatsSketchMerge";
  public static final ColumnType FLOATS_TYPE = ColumnType.ofComplex(FLOATS_SKETCH);
  public static final ColumnType FLOATS_MERGE_TYPE = ColumnType.ofComplex(FLOATS_SKETCH_MERGE);

  public static final String FLOATS_SKETCH_HISTOGRAM_POST_AGG = "KllFloatsSketchToHistogram";
  public static final String FLOATS_SKETCH_QUANTILE_POST_AGG = "KllFloatsSketchToQuantile";
  public static final String FLOATS_SKETCH_QUANTILES_POST_AGG = "KllFloatsSketchToQuantiles";
  public static final String FLOATS_SKETCH_RANK_POST_AGG = "KllFloatsSketchToRank";
  public static final String FLOATS_SKETCH_CDF_POST_AGG = "KllFloatsSketchToCDF";
  public static final String FLOATS_SKETCH_TO_STRING_POST_AGG = "KllFloatsSketchToString";

  @Override
  public void configure(final Binder binder)
  {
    registerSerde();
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.<Module>unmodifiableList(Arrays.asList(
        new SimpleModule("KllDoublesSketchModule")
            .registerSubtypes(
                new NamedType(KllDoublesSketchAggregatorFactory.class, DOUBLES_SKETCH),
                new NamedType(KllDoublesSketchMergeAggregatorFactory.class, DOUBLES_SKETCH_MERGE),
                new NamedType(KllDoublesSketchToHistogramPostAggregator.class, DOUBLES_SKETCH_HISTOGRAM_POST_AGG),
                new NamedType(KllDoublesSketchToQuantilePostAggregator.class, DOUBLES_SKETCH_QUANTILE_POST_AGG),
                new NamedType(KllDoublesSketchToQuantilesPostAggregator.class, DOUBLES_SKETCH_QUANTILES_POST_AGG),
                new NamedType(KllDoublesSketchToRankPostAggregator.class, DOUBLES_SKETCH_RANK_POST_AGG),
                new NamedType(KllDoublesSketchToCDFPostAggregator.class, DOUBLES_SKETCH_CDF_POST_AGG),
                new NamedType(KllDoublesSketchToStringPostAggregator.class, DOUBLES_SKETCH_TO_STRING_POST_AGG)
            ).addSerializer(KllDoublesSketch.class, new KllDoublesSketchJsonSerializer()),
        new SimpleModule("KllFloatsSketchModule")
            .registerSubtypes(
                new NamedType(KllFloatsSketchAggregatorFactory.class, FLOATS_SKETCH),
                new NamedType(KllFloatsSketchMergeAggregatorFactory.class, FLOATS_SKETCH_MERGE),
                new NamedType(KllFloatsSketchToHistogramPostAggregator.class, FLOATS_SKETCH_HISTOGRAM_POST_AGG),
                new NamedType(KllFloatsSketchToQuantilePostAggregator.class, FLOATS_SKETCH_QUANTILE_POST_AGG),
                new NamedType(KllFloatsSketchToQuantilesPostAggregator.class, FLOATS_SKETCH_QUANTILES_POST_AGG),
                new NamedType(KllFloatsSketchToRankPostAggregator.class, FLOATS_SKETCH_RANK_POST_AGG),
                new NamedType(KllFloatsSketchToCDFPostAggregator.class, FLOATS_SKETCH_CDF_POST_AGG),
                new NamedType(KllFloatsSketchToStringPostAggregator.class, FLOATS_SKETCH_TO_STRING_POST_AGG)
            ).addSerializer(KllFloatsSketch.class, new KllFloatsSketchJsonSerializer())
    ));
  }

  @VisibleForTesting
  public static void registerSerde()
  {
    ComplexMetrics.registerSerde(DOUBLES_SKETCH, new KllDoublesSketchComplexMetricSerde());
    ComplexMetrics.registerSerde(FLOATS_SKETCH, new KllFloatsSketchComplexMetricSerde());
  }
}
