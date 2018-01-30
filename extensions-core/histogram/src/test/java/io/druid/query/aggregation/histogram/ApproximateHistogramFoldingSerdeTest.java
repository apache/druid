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

package io.druid.query.aggregation.histogram;

import com.google.common.collect.ImmutableList;
import io.druid.data.input.MapBasedInputRow;
import io.druid.segment.serde.ComplexMetricExtractor;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ApproximateHistogramFoldingSerdeTest
{
  @Test
  public void testExtractor()
  {
    final ApproximateHistogramFoldingSerde serde = new ApproximateHistogramFoldingSerde();
    final ComplexMetricExtractor extractor = serde.getExtractor();

    final Map<String, Object> theMap = new HashMap<>();
    theMap.put("nullValue", null);
    theMap.put("listValue", ImmutableList.of("1.0", 2, 3.0));
    theMap.put("stringValue", "1.0");
    theMap.put("numberValue", 1.0);

    final MapBasedInputRow row = new MapBasedInputRow(0L, ImmutableList.of(), theMap);

    Assert.assertEquals(
        "nullValue",
        new ApproximateHistogram(0),
        extractor.extractValue(row, "nullValue")
    );

    Assert.assertEquals(
        "missingValue",
        new ApproximateHistogram(0),
        extractor.extractValue(row, "missingValue")
    );

    Assert.assertEquals(
        "listValue",
        makeHistogram(1, 2, 3),
        extractor.extractValue(row, "listValue")
    );

    Assert.assertEquals(
        "stringValue",
        makeHistogram(1),
        extractor.extractValue(row, "stringValue")
    );

    Assert.assertEquals(
        "numberValue",
        makeHistogram(1),
        extractor.extractValue(row, "numberValue")
    );
  }

  public static ApproximateHistogram makeHistogram(final float... floats)
  {
    final ApproximateHistogram histogram = new ApproximateHistogram();
    for (float f : floats) {
      histogram.offer(f);
    }
    return histogram;
  }
}
