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

package org.apache.druid.data.input;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MapPopulatorTest
{
  private static long EIGHT_BYTES = 8L;
  private static long FOUR_BYTES = 4L;

  @Test
  public void test_getByteLengthOfObject_string_stringLength()
  {
    String o = "string";
    Assert.assertEquals((o.length() * Character.BYTES) + 40, MapPopulator.getByteLengthOfObject(o));
  }

  @Test
  public void test_getByteLengthOfObject_double_8()
  {
    Assert.assertEquals(EIGHT_BYTES, MapPopulator.getByteLengthOfObject(12.0));
  }

  @Test
  public void test_getByteLengthOfObject_float_4()
  {
    Assert.assertEquals(FOUR_BYTES, MapPopulator.getByteLengthOfObject(12.0F));
  }

  @Test
  public void test_getByteLengthOfObject_int_4()
  {
    Assert.assertEquals(FOUR_BYTES, MapPopulator.getByteLengthOfObject(12));
  }

  @Test
  public void test_getByteLengthOfObject_long_8()
  {
    Assert.assertEquals(EIGHT_BYTES, MapPopulator.getByteLengthOfObject(12L));
  }

  @Test
  public void test_getByteLengthOfObject_null_0()
  {
    Assert.assertEquals(0, MapPopulator.getByteLengthOfObject(null));
  }

  @Test
  public void test_getByteLengthOfObject_map_0()
  {
    Assert.assertEquals(0, MapPopulator.getByteLengthOfObject(ImmutableMap.of()));
  }

  @Test
  public void test_canKeyAndValueTypesByteSizesBeDetermined_stringKeyAndStringValue_true()
  {
    Assert.assertTrue(MapPopulator.canKeyAndValueTypesByteSizesBeDetermined("key", "value"));
  }

  @Test
  public void test_canKeyAndValueTypesByteSizesBeDetermined_doubleKeyAndDoubleValue_true()
  {
    Assert.assertTrue(MapPopulator.canKeyAndValueTypesByteSizesBeDetermined(1.0, 2.0));
  }

  @Test
  public void test_canKeyAndValueTypesByteSizesBeDetermined_floatKeyAndFloatValue_true()
  {
    Assert.assertTrue(MapPopulator.canKeyAndValueTypesByteSizesBeDetermined(1.0F, 2.0F));
  }

  @Test
  public void test_canKeyAndValueTypesByteSizesBeDetermined_intKeyAndIntValue_true()
  {
    Assert.assertTrue(MapPopulator.canKeyAndValueTypesByteSizesBeDetermined(1, 2));
  }

  @Test
  public void test_canKeyAndValueTypesByteSizesBeDetermined_longKeyAndLongValue_true()
  {
    Assert.assertTrue(MapPopulator.canKeyAndValueTypesByteSizesBeDetermined(1L, 2L));
  }

  @Test
  public void test_canKeyAndValueTypesByteSizesBeDetermined_nullKeyAndNullValue_true()
  {
    Assert.assertTrue(MapPopulator.canKeyAndValueTypesByteSizesBeDetermined(null, null));
  }

  @Test
  public void test_canKeyAndValueTypesByteSizesBeDetermined_mapKeyAndmapValue_false()
  {
    Assert.assertFalse(MapPopulator.canKeyAndValueTypesByteSizesBeDetermined(ImmutableMap.of(), ImmutableMap.of()));
  }

  @Test
  public void test_canKeyAndValueTypesByteSizesBeDetermined_nullKeyAndmapValue_false()
  {
    Assert.assertFalse(MapPopulator.canKeyAndValueTypesByteSizesBeDetermined(null, ImmutableMap.of()));
  }

  @Test
  public void test_canKeyAndValueTypesByteSizesBeDetermined_mapKeyAndNullValue_false()
  {
    Assert.assertFalse(MapPopulator.canKeyAndValueTypesByteSizesBeDetermined(ImmutableMap.of(), null));
  }

  @Test
  public void test_populateAndWarnAtByteLimit_empty_succeeds()
  {
    Set<Pair<String, String>> pairs = ImmutableSet.of();
    Map<String, String> map = new HashMap<>();
    MapPopulator.PopulateResult result =
        MapPopulator.populateAndWarnAtByteLimit(pairs.iterator(), map, -1, "");
    Assert.assertTrue(map.isEmpty());
    Assert.assertEquals(0, result.getEntries());
    Assert.assertEquals(0, result.getLines());
    Assert.assertEquals(0L, result.getBytes());
  }

  @Test
  public void test_populateAndWarnAtByteLimit_pairTypesBytesComputableAndNoByteLimit_succeeds()
  {
    Set<Pair<Object, Object>> pairs = new HashSet<>();
    pairs.add(new Pair<>("key1", "val1"));
    pairs.add(new Pair<>("key2", "val2"));
    pairs.add(new Pair<>(null, null));
    pairs.add(null);
    Map<Object, Object> expectedMap = new HashMap<>();
    expectedMap.put("key1", "val1");
    expectedMap.put("key2", "val2");
    expectedMap.put(null, null);
    Map<Object, Object> map = new HashMap<>();
    MapPopulator.PopulateResult result =
        MapPopulator.populateAndWarnAtByteLimit(pairs.iterator(), map, -1, null);
    Assert.assertEquals(expectedMap, map);
    Assert.assertEquals(4, result.getEntries());
    Assert.assertEquals(0, result.getLines());
    Assert.assertEquals(0L, result.getBytes());
  }

  @Test
  public void test_populateAndWarnAtByteLimit_pairTypesBytesComputableAndByteLimit_succeeds()
  {
    Set<Pair<Object, Object>> pairs = new HashSet<>();
    pairs.add(new Pair<>("key1", "val1"));
    pairs.add(new Pair<>("key2", "val2"));
    pairs.add(new Pair<>(null, null));
    pairs.add(null);
    Map<Object, Object> expectedMap = new HashMap<>();
    expectedMap.put("key1", "val1");
    expectedMap.put("key2", "val2");
    expectedMap.put(null, null);
    Map<Object, Object> map = new HashMap<>();
    MapPopulator.PopulateResult result =
        MapPopulator.populateAndWarnAtByteLimit(pairs.iterator(), map, 10, null);
    Assert.assertEquals(expectedMap, map);
    Assert.assertEquals(4, result.getEntries());
    Assert.assertEquals(0, result.getLines());
    Assert.assertEquals(192L, result.getBytes());
  }

  @Test
  public void test_populateAndWarnAtByteLimit_pairTypesBytesNotComputableAndByteLimit_succeeds()
  {
    Set<Pair<Object, Object>> pairs = new HashSet<>();
    pairs.add(new Pair<>(ImmutableSet.of(1), ImmutableSet.of(2)));
    pairs.add(new Pair<>(ImmutableSet.of(3), ImmutableSet.of(4)));
    Map<Object, Object> expectedMap = new HashMap<>();
    expectedMap.put(ImmutableSet.of(1), ImmutableSet.of(2));
    expectedMap.put(ImmutableSet.of(3), ImmutableSet.of(4));
    Map<Object, Object> map = new HashMap<>();
    MapPopulator.PopulateResult result =
        MapPopulator.populateAndWarnAtByteLimit(pairs.iterator(), map, 10, null);
    Assert.assertEquals(expectedMap, map);
    Assert.assertEquals(2, result.getEntries());
    Assert.assertEquals(0, result.getLines());
    Assert.assertEquals(0L, result.getBytes());
  }
}
