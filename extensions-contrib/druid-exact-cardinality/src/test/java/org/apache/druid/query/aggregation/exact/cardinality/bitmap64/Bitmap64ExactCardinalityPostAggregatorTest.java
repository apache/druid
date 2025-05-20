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

package org.apache.druid.query.aggregation.exact.cardinality.bitmap64;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class Bitmap64ExactCardinalityPostAggregatorTest
{
  private static final String NAME = "postAggTestName";
  private static final String FIELD_NAME = "postAggTestFieldName";

  private Bitmap64ExactCardinalityPostAggregator postAggregator;

  @BeforeEach
  public void setUp()
  {
    postAggregator = new Bitmap64ExactCardinalityPostAggregator(NAME, FIELD_NAME);
  }

  @Test
  public void testConstructor()
  {
    Assertions.assertEquals(NAME, postAggregator.getName());
    Assertions.assertEquals(FIELD_NAME, postAggregator.getFieldName());
  }

  @Test
  public void testGetDependentFields()
  {
    Assertions.assertEquals(Collections.singleton(FIELD_NAME), postAggregator.getDependentFields());
  }

  @Test
  public void testGetComparator()
  {
    Assertions.assertEquals(ArithmeticPostAggregator.DEFAULT_COMPARATOR, postAggregator.getComparator());
  }

  @Test
  public void testCompute()
  {
    Bitmap64Counter counter = new RoaringBitmap64Counter();
    counter.add(1L);
    counter.add(2L);
    counter.add(3L);

    Map<String, Object> combinedAggregators = ImmutableMap.of(FIELD_NAME, counter);
    Assertions.assertEquals(3L, postAggregator.compute(combinedAggregators));
    
    Bitmap64Counter emptyCounter = new RoaringBitmap64Counter();
    Map<String, Object> combinedAggregatorsEmpty = ImmutableMap.of(FIELD_NAME, emptyCounter);
    Assertions.assertEquals(0L, postAggregator.compute(combinedAggregatorsEmpty));
  }

  @Test
  public void testGetType()
  {
    Assertions.assertEquals(ColumnType.LONG, postAggregator.getType(null)); 
  }

  @Test
  public void testDecorate()
  {
    Assertions.assertSame(postAggregator, postAggregator.decorate(Collections.emptyMap()));
  }

  @Test
  public void testGetCacheKey()
  {
    byte[] expectedKey = new CacheKeyBuilder(PostAggregatorIds.BITMAP64_EXACT_CARDINALITY_TYPE_ID)
        .appendString(FIELD_NAME)
        .build();
    Assertions.assertArrayEquals(expectedKey, postAggregator.getCacheKey());

    Bitmap64ExactCardinalityPostAggregator postAggregator2 = new Bitmap64ExactCardinalityPostAggregator(NAME, FIELD_NAME);
    Assertions.assertArrayEquals(postAggregator.getCacheKey(), postAggregator2.getCacheKey());

    Bitmap64ExactCardinalityPostAggregator postAggregatorDiffFieldName = new Bitmap64ExactCardinalityPostAggregator(NAME, FIELD_NAME + "_diff");
    Assertions.assertFalse(Arrays.equals(postAggregator.getCacheKey(), postAggregatorDiffFieldName.getCacheKey()));
    
    Bitmap64ExactCardinalityPostAggregator postAggregatorDiffName = new Bitmap64ExactCardinalityPostAggregator(NAME + "_diff", FIELD_NAME);
    // Cache key for PostAggregator does not include its own name, only dependent fieldName
    Assertions.assertArrayEquals(postAggregator.getCacheKey(), postAggregatorDiffName.getCacheKey()); 
  }
  
  @Test
  public void testEqualsAndHashCode()
  {
    Bitmap64ExactCardinalityPostAggregator pa1 = new Bitmap64ExactCardinalityPostAggregator(NAME, FIELD_NAME);
    Bitmap64ExactCardinalityPostAggregator pa2 = new Bitmap64ExactCardinalityPostAggregator(NAME, FIELD_NAME);
    Bitmap64ExactCardinalityPostAggregator paDiffName = new Bitmap64ExactCardinalityPostAggregator(NAME + "_diff", FIELD_NAME);
    Bitmap64ExactCardinalityPostAggregator paDiffFieldName = new Bitmap64ExactCardinalityPostAggregator(NAME, FIELD_NAME + "_diff");

    Assertions.assertEquals(pa1, pa2);
    Assertions.assertEquals(pa1.hashCode(), pa2.hashCode());

    Assertions.assertNotEquals(null, pa1); // Null comparison
    Assertions.assertNotEquals(new Object(), pa1); // Different type

    Assertions.assertNotEquals(pa1, paDiffName);
    Assertions.assertNotEquals(pa1.hashCode(), paDiffName.hashCode());

    Assertions.assertNotEquals(pa1, paDiffFieldName);
    Assertions.assertNotEquals(pa1.hashCode(), paDiffFieldName.hashCode());
  }

  @Test
  public void testToString()
  {
    String expected = "Bitmap64ExactCardinalityPostAggregator{name='" + NAME + "', field=" + FIELD_NAME + "}";
    Assertions.assertEquals(expected, postAggregator.toString());
  }
} 