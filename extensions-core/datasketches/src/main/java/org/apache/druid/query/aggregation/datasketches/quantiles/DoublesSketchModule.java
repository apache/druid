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

package org.apache.druid.query.aggregation.datasketches.quantiles;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import com.yahoo.sketches.quantiles.DoublesSketch;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.datasketches.quantiles.sql.DoublesSketchApproxQuantileSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.sql.DoublesSketchObjectSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.sql.DoublesSketchQuantileOperatorConversion;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.Collections;
import java.util.List;

public class DoublesSketchModule implements DruidModule
{

  public static final String DOUBLES_SKETCH = "quantilesDoublesSketch";
  public static final String DOUBLES_SKETCH_MERGE = "quantilesDoublesSketchMerge";

  public static final String DOUBLES_SKETCH_HISTOGRAM_POST_AGG = "quantilesDoublesSketchToHistogram";
  public static final String DOUBLES_SKETCH_QUANTILE_POST_AGG = "quantilesDoublesSketchToQuantile";
  public static final String DOUBLES_SKETCH_QUANTILES_POST_AGG = "quantilesDoublesSketchToQuantiles";
  public static final String DOUBLES_SKETCH_RANK_POST_AGG = "quantilesDoublesSketchToRank";
  public static final String DOUBLES_SKETCH_CDF_POST_AGG = "quantilesDoublesSketchToCDF";
  public static final String DOUBLES_SKETCH_TO_STRING_POST_AGG = "quantilesDoublesSketchToString";

  @Override
  public void configure(final Binder binder)
  {
    registerSerde();
    SqlBindings.addAggregator(binder, DoublesSketchApproxQuantileSqlAggregator.class);
    SqlBindings.addAggregator(binder, DoublesSketchObjectSqlAggregator.class);

    SqlBindings.addOperatorConversion(binder, DoublesSketchQuantileOperatorConversion.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.<Module>singletonList(
        new SimpleModule("DoublesQuantilesSketchModule")
            .registerSubtypes(
                new NamedType(DoublesSketchAggregatorFactory.class, DOUBLES_SKETCH),
                new NamedType(DoublesSketchMergeAggregatorFactory.class, DOUBLES_SKETCH_MERGE),
                new NamedType(DoublesSketchToHistogramPostAggregator.class, DOUBLES_SKETCH_HISTOGRAM_POST_AGG),
                new NamedType(DoublesSketchToQuantilePostAggregator.class, DOUBLES_SKETCH_QUANTILE_POST_AGG),
                new NamedType(DoublesSketchToQuantilesPostAggregator.class, DOUBLES_SKETCH_QUANTILES_POST_AGG),
                new NamedType(DoublesSketchToRankPostAggregator.class, DOUBLES_SKETCH_RANK_POST_AGG),
                new NamedType(DoublesSketchToCDFPostAggregator.class, DOUBLES_SKETCH_CDF_POST_AGG),
                new NamedType(DoublesSketchToStringPostAggregator.class, DOUBLES_SKETCH_TO_STRING_POST_AGG)
            ).addSerializer(DoublesSketch.class, new DoublesSketchJsonSerializer())
    );
  }

  @VisibleForTesting
  public static void registerSerde()
  {
    ComplexMetrics.registerSerde(DOUBLES_SKETCH, new DoublesSketchComplexMetricSerde());
  }
}
