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

package io.druid.query.groupby.having;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


public class DimensionSelectorHavingSpecTest
{

  private static final byte CACHE_KEY = (byte) 0x8;
  private static final byte STRING_SEPARATOR = (byte) 0xFF;

  private Row getTestRow(Object dimensionValue)
  {
    return new MapBasedRow(0, ImmutableMap.of("dimension", dimensionValue));
  }

  @Test
  public void testDimSelectorHavingClauseSerde() throws Exception
  {
    HavingSpec dimHavingSpec = new DimensionSelectorHavingSpec("dim", "v");

    Map<String, Object> dimSelectMap = ImmutableMap.<String, Object>of(
        "type", "dimSelector",
        "dimension", "dim",
        "value", "v"
    );

    ObjectMapper mapper = new DefaultObjectMapper();
    assertEquals(dimHavingSpec, mapper.convertValue(dimSelectMap, DimensionSelectorHavingSpec.class));

  }

  @Test
  public void testEquals() throws Exception
  {
    HavingSpec dimHavingSpec1 = new DimensionSelectorHavingSpec("dim", "v");
    HavingSpec dimHavingSpec2 = new DimensionSelectorHavingSpec("dim", "v");
    HavingSpec dimHavingSpec3 = new DimensionSelectorHavingSpec("dim1", "v");
    HavingSpec dimHavingSpec4 = new DimensionSelectorHavingSpec("dim2", "v");
    HavingSpec dimHavingSpec5 = new DimensionSelectorHavingSpec("dim", "v1");
    HavingSpec dimHavingSpec6 = new DimensionSelectorHavingSpec("dim", "v2");
    HavingSpec dimHavingSpec7 = new DimensionSelectorHavingSpec("dim", null);
    HavingSpec dimHavingSpec8 = new DimensionSelectorHavingSpec("dim", null);
    HavingSpec dimHavingSpec9 = new DimensionSelectorHavingSpec("dim1", null);
    HavingSpec dimHavingSpec10 = new DimensionSelectorHavingSpec("dim2", null);
    HavingSpec dimHavingSpec11 = new DimensionSelectorHavingSpec("dim1", "v");
    HavingSpec dimHavingSpec12 = new DimensionSelectorHavingSpec("dim2", null);

    assertEquals(dimHavingSpec1, dimHavingSpec2);
    assertNotEquals(dimHavingSpec3, dimHavingSpec4);
    assertNotEquals(dimHavingSpec5, dimHavingSpec6);
    assertEquals(dimHavingSpec7, dimHavingSpec8);
    assertNotEquals(dimHavingSpec9, dimHavingSpec10);
    assertNotEquals(dimHavingSpec11, dimHavingSpec12);

  }

  @Test
  public void testToString()
  {
    String expected = "DimensionSelectorHavingSpec{dimension='dimension', value='v'}";

    Assert.assertEquals(new DimensionSelectorHavingSpec("dimension", "v").toString(), expected);
  }

  @Test(expected = NullPointerException.class)
  public void testNullDimension()
  {
    new DimensionSelectorHavingSpec(null, "value");
  }

  @Test
  public void testDimensionFilterSpec()
  {
    DimensionSelectorHavingSpec spec = new DimensionSelectorHavingSpec("dimension", "v");
    assertTrue(spec.eval(getTestRow("v")));
    assertTrue(spec.eval(getTestRow(ImmutableList.of("v", "v1"))));
    assertFalse(spec.eval(getTestRow(ImmutableList.of())));
    assertFalse(spec.eval(getTestRow("v1")));

    spec = new DimensionSelectorHavingSpec("dimension", null);
    assertTrue(spec.eval(getTestRow(ImmutableList.of())));
    assertTrue(spec.eval(getTestRow(ImmutableList.of(""))));
    assertFalse(spec.eval(getTestRow(ImmutableList.of("v"))));
    assertFalse(spec.eval(getTestRow(ImmutableList.of("v", "v1"))));

    spec = new DimensionSelectorHavingSpec("dimension", "");
    assertTrue(spec.eval(getTestRow(ImmutableList.of())));
    assertTrue(spec.eval(getTestRow(ImmutableList.of(""))));
    assertTrue(spec.eval(getTestRow(ImmutableList.of("v", "v1", ""))));
    assertFalse(spec.eval(getTestRow(ImmutableList.of("v"))));
    assertFalse(spec.eval(getTestRow(ImmutableList.of("v", "v1"))));
  }

  @Test
  public void testGetCacheKey()
  {
    byte[] dimBytes = "dimension".getBytes(Charsets.UTF_8);
    byte[] valBytes = "v".getBytes(Charsets.UTF_8);

    byte[] expected = ByteBuffer.allocate(12)
                                .put(CACHE_KEY)
                                .put(dimBytes)
                                .put(STRING_SEPARATOR)
                                .put(valBytes)
                                .array();

    DimensionSelectorHavingSpec dfhs = new DimensionSelectorHavingSpec("dimension", "v");
    DimensionSelectorHavingSpec dfhs1 = new DimensionSelectorHavingSpec("dimension", "v");
    DimensionSelectorHavingSpec dfhs2 = new DimensionSelectorHavingSpec("dimensi", "onv");

    byte[] actual = dfhs.getCacheKey();

    Assert.assertArrayEquals(expected, actual);
    Assert.assertTrue(Arrays.equals(dfhs.getCacheKey(), dfhs1.getCacheKey()));
    Assert.assertFalse(Arrays.equals(dfhs.getCacheKey(), dfhs2.getCacheKey()));


  }
}
