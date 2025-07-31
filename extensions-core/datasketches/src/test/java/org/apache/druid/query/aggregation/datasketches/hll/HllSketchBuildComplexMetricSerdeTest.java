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

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.junit.Assert;
import org.junit.Test;

public class HllSketchBuildComplexMetricSerdeTest
{

  @Test
  public void testComplexSerdeToBytesOnRealtimeSegmentSketch()
  {
    ComplexMetrics.registerSerde(HllSketchModule.BUILD_TYPE_NAME, new HllSketchBuildComplexMetricSerde());
    ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(HllSketchModule.BUILD_TYPE_NAME);
    Assert.assertNotNull(serde);
    HllSketch sketchNew = new HllSketch(14, TgtHllType.HLL_8);
    HllSketchBuildUtil.updateSketch(sketchNew, StringEncoding.UTF16LE, new int[]{1, 2});

    HllSketchHolder sketchHolder = HllSketchHolder.of(sketchNew);

    byte[] bytes = serde.toBytes(sketchHolder);
    Assert.assertEquals(bytes, serde.toBytes(bytes));

    HllSketchHolder fromBytesHolder = (HllSketchHolder) serde.fromBytes(bytes, 0, bytes.length);

    Assert.assertEquals(sketchHolder.getSketch().getLgConfigK(), fromBytesHolder.getSketch().getLgConfigK());
    Assert.assertEquals(sketchHolder.getSketch().getTgtHllType(), fromBytesHolder.getSketch().getTgtHllType());
    Assert.assertEquals(
        sketchHolder.getSketch().getCompactSerializationBytes(),
        fromBytesHolder.getSketch().getCompactSerializationBytes()
    );
    Assert.assertEquals(
        sketchHolder.getSketch().getUpdatableSerializationBytes(),
        fromBytesHolder.getSketch().getUpdatableSerializationBytes()
    );
    Assert.assertEquals(sketchHolder.getSketch().getEstimate(), fromBytesHolder.getSketch().getEstimate(), 0);
    Assert.assertEquals(sketchHolder.getSketch().getLowerBound(1), fromBytesHolder.getSketch().getLowerBound(1), 0);
    Assert.assertEquals(sketchHolder.getSketch().getUpperBound(1), fromBytesHolder.getSketch().getUpperBound(1), 0);

    // During fromBytes() sketches field memory changes to TRUE
    Assert.assertFalse(sketchHolder.getSketch().isMemory());
    Assert.assertTrue(fromBytesHolder.getSketch().isMemory());
  }
}
