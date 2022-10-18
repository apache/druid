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

package org.apache.druid.compressedbigdecimal;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class CompressedBigDecimalCachingTest
{
  private static final Object FLAG = new Object();

  @Test
  public void testCrossFactory()
  {
    String name = "name";
    String fieldName = "fieldName";
    int size = 10;
    int scale = 3;
    boolean strictNumberParsing = true;
    AggregatorFactory aggregatorFactory1 = new CompressedBigDecimalMaxAggregatorFactory(
        name,
        fieldName,
        size,
        scale,
        strictNumberParsing
    );
    AggregatorFactory aggregatorFactory2 = new CompressedBigDecimalMinAggregatorFactory(
        name,
        fieldName,
        size,
        scale,
        strictNumberParsing
    );
    AggregatorFactory aggregatorFactory3 = new CompressedBigDecimalSumAggregatorFactory(
        name,
        fieldName,
        size,
        scale,
        strictNumberParsing
    );

    Map<String, Object> cache = new HashMap<>();

    cache.put(Base64.getEncoder().encodeToString(aggregatorFactory1.getCacheKey()), FLAG);
    cache.put(Base64.getEncoder().encodeToString(aggregatorFactory2.getCacheKey()), FLAG);
    cache.put(Base64.getEncoder().encodeToString(aggregatorFactory3.getCacheKey()), FLAG);

    Assert.assertEquals(3, cache.size());
  }
}
