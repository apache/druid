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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class KllDoublesSketchComplexMetricSerdeTest
{
  @Test
  public void testExtractorOnEmptyString()
  {
    final KllDoublesSketchComplexMetricSerde serde = new KllDoublesSketchComplexMetricSerde();
    final ComplexMetricExtractor extractor = serde.getExtractor();
    final KllDoublesSketch sketch = (KllDoublesSketch) extractor.extractValue(
        new MapBasedInputRow(0L, ImmutableList.of(), ImmutableMap.of("foo", "")),
        "foo"
    );
    Assert.assertEquals(0, sketch.getNumRetained());
  }

  @Test
  public void testExtractorOnPositiveNumber()
  {
    final KllDoublesSketchComplexMetricSerde serde = new KllDoublesSketchComplexMetricSerde();
    final ComplexMetricExtractor extractor = serde.getExtractor();
    final KllDoublesSketch sketch = (KllDoublesSketch) extractor.extractValue(
        new MapBasedInputRow(0L, ImmutableList.of(), ImmutableMap.of("foo", "777")),
        "foo"
    );
    Assert.assertEquals(1, sketch.getNumRetained());
    Assert.assertEquals(777d, sketch.getMaxValue(), 0.01d);
  }

  @Test
  public void testExtractorOnNegativeNumber()
  {
    final KllDoublesSketchComplexMetricSerde serde = new KllDoublesSketchComplexMetricSerde();
    final ComplexMetricExtractor extractor = serde.getExtractor();
    final KllDoublesSketch sketch = (KllDoublesSketch) extractor.extractValue(
        new MapBasedInputRow(0L, ImmutableList.of(), ImmutableMap.of("foo", "-133")),
        "foo"
    );
    Assert.assertEquals(1, sketch.getNumRetained());
    Assert.assertEquals(-133d, sketch.getMaxValue(), 0.01d);
  }

  @Test
  public void testExtractorOnDecimalNumber()
  {
    final KllDoublesSketchComplexMetricSerde serde = new KllDoublesSketchComplexMetricSerde();
    final ComplexMetricExtractor extractor = serde.getExtractor();
    final KllDoublesSketch sketch = (KllDoublesSketch) extractor.extractValue(
        new MapBasedInputRow(0L, ImmutableList.of(), ImmutableMap.of("foo", "3.1")),
        "foo"
    );
    Assert.assertEquals(1, sketch.getNumRetained());
    Assert.assertEquals(3.1d, sketch.getMaxValue(), 0.01d);
  }

  @Test
  public void testExtractorOnLeadingDecimalNumber()
  {
    final KllDoublesSketchComplexMetricSerde serde = new KllDoublesSketchComplexMetricSerde();
    final ComplexMetricExtractor extractor = serde.getExtractor();
    final KllDoublesSketch sketch = (KllDoublesSketch) extractor.extractValue(
        new MapBasedInputRow(0L, ImmutableList.of(), ImmutableMap.of("foo", ".1")),
        "foo"
    );
    Assert.assertEquals(1, sketch.getNumRetained());
    Assert.assertEquals(0.1d, sketch.getMaxValue(), 0.01d);
  }

  @Test
  public void testSafeRead()
  {
    final KllDoublesSketchComplexMetricSerde serde = new KllDoublesSketchComplexMetricSerde();
    final ObjectStrategy<KllDoublesSketch> objectStrategy = serde.getObjectStrategy();

    KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
    sketch.update(1.1);
    sketch.update(1.2);
    final byte[] bytes = sketch.toByteArray();

    ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

    // valid sketch should not explode when converted to byte array, which reads the memory
    objectStrategy.fromByteBufferSafe(buf, bytes.length).toByteArray();

    // corrupted sketch should fail with a regular java buffer exception, not all subsets actually fail with the same
    // index out of bounds exceptions, but at least this many do
    for (int subset = 3; subset < 24; subset++) {
      final byte[] garbage2 = new byte[subset];
      for (int i = 0; i < garbage2.length; i++) {
        garbage2[i] = buf.get(i);
      }

      final ByteBuffer buf2 = ByteBuffer.wrap(garbage2).order(ByteOrder.LITTLE_ENDIAN);
      Assert.assertThrows(
          IndexOutOfBoundsException.class,
          () -> objectStrategy.fromByteBufferSafe(buf2, garbage2.length).toByteArray()
      );
    }

    // non sketch that is too short to contain header should fail with regular java buffer exception
    final byte[] garbage = new byte[]{0x01, 0x02};
    final ByteBuffer buf3 = ByteBuffer.wrap(garbage).order(ByteOrder.LITTLE_ENDIAN);
    Assert.assertThrows(
        IndexOutOfBoundsException.class,
        () -> objectStrategy.fromByteBufferSafe(buf3, garbage.length).toByteArray()
    );
  }
}
