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

package org.apache.druid.query.aggregation.datasketches.tuple;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.serde.ComplexMetrics;

import java.util.Collections;
import java.util.List;

/**
 * This module is to support numeric Tuple sketches, which extend the functionality of the count-distinct
 * Theta sketches by adding arrays of double values associated with unique keys.
 * 
 * See <a href=https://datasketches.github.io/docs/Tuple/TupleOverview.html>Tuple Sketch Overview</a>
 */
public class ArrayOfDoublesSketchModule implements DruidModule
{

  public static final String ARRAY_OF_DOUBLES_SKETCH = "arrayOfDoublesSketch";

  public static final String ARRAY_OF_DOUBLES_SKETCH_MERGE_AGG = "arrayOfDoublesSketchMerge";
  public static final String ARRAY_OF_DOUBLES_SKETCH_BUILD_AGG = "arrayOfDoublesSketchBuild";

  @Override
  public void configure(final Binder binder)
  {
    ComplexMetrics.registerSerde(ARRAY_OF_DOUBLES_SKETCH, new ArrayOfDoublesSketchMergeComplexMetricSerde());
    ComplexMetrics.registerSerde(ARRAY_OF_DOUBLES_SKETCH_MERGE_AGG, new ArrayOfDoublesSketchMergeComplexMetricSerde());
    ComplexMetrics.registerSerde(ARRAY_OF_DOUBLES_SKETCH_BUILD_AGG, new ArrayOfDoublesSketchBuildComplexMetricSerde());
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.<Module>singletonList(
        new SimpleModule("ArrayOfDoublesSketchModule").registerSubtypes(
            new NamedType(
                ArrayOfDoublesSketchAggregatorFactory.class,
                ARRAY_OF_DOUBLES_SKETCH
            ),
            new NamedType(
                ArrayOfDoublesSketchToEstimatePostAggregator.class,
                "arrayOfDoublesSketchToEstimate"
            ),
            new NamedType(
                ArrayOfDoublesSketchToEstimateAndBoundsPostAggregator.class,
                "arrayOfDoublesSketchToEstimateAndBounds"
            ),
            new NamedType(
                ArrayOfDoublesSketchToNumEntriesPostAggregator.class,
                "arrayOfDoublesSketchToNumEntries"
            ),
            new NamedType(
                ArrayOfDoublesSketchToMeansPostAggregator.class,
                "arrayOfDoublesSketchToMeans"
            ),
            new NamedType(
                ArrayOfDoublesSketchToVariancesPostAggregator.class,
                "arrayOfDoublesSketchToVariances"
            ),
            new NamedType(
                ArrayOfDoublesSketchToQuantilesSketchPostAggregator.class,
                "arrayOfDoublesSketchToQuantilesSketch"
            ),
            new NamedType(
                ArrayOfDoublesSketchSetOpPostAggregator.class,
                "arrayOfDoublesSketchSetOp"
            ),
            new NamedType(
                ArrayOfDoublesSketchTTestPostAggregator.class,
                "arrayOfDoublesSketchTTest"
            ),
            new NamedType(
                ArrayOfDoublesSketchToStringPostAggregator.class,
                "arrayOfDoublesSketchToString"
            )
        ).addSerializer(ArrayOfDoublesSketch.class, new ArrayOfDoublesSketchJsonSerializer())
    );
  }

}
