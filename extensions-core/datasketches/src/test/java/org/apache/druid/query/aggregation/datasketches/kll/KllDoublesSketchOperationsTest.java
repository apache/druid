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

import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class KllDoublesSketchOperationsTest
{
  @Test
  public void testDeserializeSafe()
  {
    KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
    sketch.update(1.1);
    sketch.update(1.2);
    final byte[] bytes = sketch.toByteArray();
    final String base64 = StringUtils.encodeBase64String(bytes);

    Assert.assertArrayEquals(bytes, KllDoublesSketchOperations.deserializeSafe(sketch).toByteArray());
    Assert.assertArrayEquals(bytes, KllDoublesSketchOperations.deserializeSafe(bytes).toByteArray());
    Assert.assertArrayEquals(bytes, KllDoublesSketchOperations.deserializeSafe(base64).toByteArray());

    final byte[] trunacted = Arrays.copyOfRange(bytes, 0, 20);
    Assert.assertThrows(IndexOutOfBoundsException.class, () -> KllDoublesSketchOperations.deserializeSafe(trunacted));
    Assert.assertThrows(
        IndexOutOfBoundsException.class,
        () -> KllDoublesSketchOperations.deserializeSafe(StringUtils.encodeBase64String(trunacted))
    );
  }
}
