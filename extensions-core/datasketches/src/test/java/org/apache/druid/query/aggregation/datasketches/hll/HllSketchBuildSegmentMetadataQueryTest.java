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

package org.apache.druid.query.aggregation.datasketches.hll;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.BaseSketchBuildSegmentMetadataQueryTest;
import org.apache.druid.segment.column.ColumnType;
import org.junit.jupiter.api.Assertions;

public class HllSketchBuildSegmentMetadataQueryTest extends BaseSketchBuildSegmentMetadataQueryTest
{
  @Override
  protected void registerSerdeAndModules(ObjectMapper jsonMapper)
  {
    HllSketchModule.registerSerde();
    jsonMapper.registerModules(new HllSketchModule().getJacksonModules());
  }

  @Override
  protected AggregatorFactory buildSketchAggregatorFactory(String sketchColumn, String inputFieldName)
  {
    return new HllSketchBuildAggregatorFactory(sketchColumn, inputFieldName, null, null, null, false, false);
  }

  @Override
  protected ColumnType expectedCanonicalColumnType()
  {
    return ColumnType.ofComplex(HllSketchModule.TYPE_NAME);
  }

  @Override
  protected void assertMergedSketchAggregator(AggregatorFactory aggregator, String sketchColumn)
  {
    Assertions.assertTrue(
        aggregator instanceof HllSketchMergeAggregatorFactory,
        "Sketch aggregator should be HllSketchMergeAggregatorFactory but was " + aggregator.getClass().getName()
    );

    HllSketchMergeAggregatorFactory hllAggregator = (HllSketchMergeAggregatorFactory) aggregator;
    Assertions.assertEquals(sketchColumn, hllAggregator.getName(), "Aggregator name should match");
    Assertions.assertEquals(sketchColumn, hllAggregator.getFieldName(), "Field name should match");
    Assertions.assertEquals(HllSketchAggregatorFactory.DEFAULT_LG_K, hllAggregator.getLgK(), "lgK should be default value");
    Assertions.assertEquals(
        TgtHllType.HLL_4.name(),
        hllAggregator.getTgtHllType(),
        "tgtHllType should be default value"
    );
    Assertions.assertEquals(
        StringEncoding.UTF16LE,
        hllAggregator.getStringEncoding(),
        "stringEncoding should be default value"
    );
  }
}
