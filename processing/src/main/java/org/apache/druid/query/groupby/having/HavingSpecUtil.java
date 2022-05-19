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

package org.apache.druid.query.groupby.having;

import com.google.common.collect.Maps;
import org.apache.druid.query.aggregation.AggregatorFactory;

import java.util.List;
import java.util.Map;

public class HavingSpecUtil
{
  static final byte CACHE_TYPE_ID_ALWAYS = 0x0;
  static final byte CACHE_TYPE_ID_AND = 0x1;
  static final byte CACHE_TYPE_ID_DIM_SELECTOR = 0x2;
  static final byte CACHE_TYPE_ID_DIM_FILTER = 0x3;
  static final byte CACHE_TYPE_ID_EQUAL = 0x4;
  static final byte CACHE_TYPE_ID_GREATER_THAN = 0x5;
  static final byte CACHE_TYPE_ID_LESS_THAN = 0x6;
  static final byte CACHE_TYPE_ID_NEVER = 0x7;
  static final byte CACHE_TYPE_ID_NOT = 0x8;
  static final byte CACHE_TYPE_ID_OR = 0x9;
  static final byte CACHE_TYPE_ID_COUNTING = 0xA;

  public static Map<String, AggregatorFactory> computeAggregatorsMap(List<AggregatorFactory> aggregatorSpecs)
  {
    Map<String, AggregatorFactory> map = Maps.newHashMapWithExpectedSize(aggregatorSpecs.size());
    aggregatorSpecs.forEach(v -> map.put(v.getName(), v));
    return map;
  }
}
