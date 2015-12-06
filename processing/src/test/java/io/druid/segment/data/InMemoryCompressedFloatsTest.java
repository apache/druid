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

package io.druid.segment.data;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteOrder;

/**
 */
public class InMemoryCompressedFloatsTest
{
  private InMemoryCompressedFloats floats;
  private float[] vals;

  @Before
  public void setUp() throws Exception
  {
    floats = null;
    vals = null;
  }

  private void setupSimple()
  {
    vals = new float[]{
        0.0f, 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 0.10f, 0.11f, 0.12f, 0.13f, 0.14f, 0.15f
    };

    floats = new InMemoryCompressedFloats(5, ByteOrder.nativeOrder());

    for (int i = 0; i < vals.length; i++) {
      Assert.assertEquals(i, floats.add(vals[i]));
    }
  }

  @Test
  public void testSanity() throws Exception
  {
    setupSimple();

    Assert.assertEquals(vals.length, this.floats.size());
    for (int i = 0; i < this.floats.size(); ++i) {
      Assert.assertEquals(vals[i], this.floats.get(i), 0.0);
    }
  }

  @Test
  public void testBulkFill() throws Exception
  {
    setupSimple();

    tryFill(0, 16);
    tryFill(3, 6);
    tryFill(7, 7);
    tryFill(7, 9);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testBulkFillTooMuch() throws Exception
  {
    setupSimple();
    tryFill(7, 10);
  }

  private void tryFill(final int startIndex, final int size)
  {
    float[] filled = new float[size];
    this.floats.fill(startIndex, filled);

    for (int i = startIndex; i < filled.length; i++) {
      Assert.assertEquals(vals[i + startIndex], filled[i], 0.0);
    }
  }

  @Test
  public void testCanConvertToCompressedFloatsIndexedSupplier() throws Exception
  {
    setupSimple();

    IndexedFloats indexed = floats.toCompressedFloatsIndexedSupplier().get();

    for (int i = 0; i < floats.size(); i++) {
      Assert.assertEquals(floats.get(i), indexed.get(i), 0.0000f);
    }

    indexed.close();
  }
}
