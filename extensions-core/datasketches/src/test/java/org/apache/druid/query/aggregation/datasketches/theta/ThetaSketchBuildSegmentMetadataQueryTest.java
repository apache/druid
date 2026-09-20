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

package org.apache.druid.query.aggregation.datasketches.theta;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.BaseSketchBuildSegmentMetadataQueryTest;
import org.apache.druid.segment.column.ColumnType;
import org.junit.jupiter.api.Assertions;

public class ThetaSketchBuildSegmentMetadataQueryTest extends BaseSketchBuildSegmentMetadataQueryTest
{
  @Override
  protected void registerSerdeAndModules(ObjectMapper jsonMapper)
  {
    SketchModule.registerSerde();
    jsonMapper.registerModules(new SketchModule().getJacksonModules());
  }

  @Override
  protected AggregatorFactory buildSketchAggregatorFactory(String sketchColumn, String inputFieldName)
  {
    // shouldFinalize=false so the aggregator represents a sketch (not a double estimate)
    // isInputThetaSketch=false to force "build" intermediate type
    return new SketchMergeAggregatorFactory(sketchColumn, inputFieldName, null, false, false, null);
  }

  @Override
  protected ColumnType expectedCanonicalColumnType()
  {
    return ColumnType.ofComplex(SketchModule.THETA_SKETCH);
  }

  @Override
  protected void assertMergedSketchAggregator(AggregatorFactory aggregator, String sketchColumn)
  {
    Assertions.assertTrue(
        aggregator instanceof SketchMergeAggregatorFactory,
        "Sketch aggregator should be SketchMergeAggregatorFactory but was " + aggregator.getClass().getName()
    );

    SketchMergeAggregatorFactory thetaAggregator = (SketchMergeAggregatorFactory) aggregator;
    Assertions.assertEquals(sketchColumn, thetaAggregator.getName(), "Aggregator name should match");
    Assertions.assertEquals(sketchColumn, thetaAggregator.getFieldName(), "Field name should match");
    Assertions.assertEquals(SketchAggregatorFactory.DEFAULT_MAX_SKETCH_SIZE, thetaAggregator.getSize(), "size should be default value");
    Assertions.assertFalse(thetaAggregator.getShouldFinalize(), "shouldFinalize should be false");
    Assertions.assertFalse(thetaAggregator.getIsInputThetaSketch(), "isInputThetaSketch should be false");
    Assertions.assertNull(thetaAggregator.getErrorBoundsStdDev(), "errorBoundsStdDev should be null");
  }
}
