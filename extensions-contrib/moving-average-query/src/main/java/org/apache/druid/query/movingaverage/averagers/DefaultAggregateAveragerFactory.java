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

package org.apache.druid.query.movingaverage.averagers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.query.aggregation.AggregatorFactory;

import java.util.Comparator;
import java.util.Map;

public class DefaultAggregateAveragerFactory extends BaseAveragerFactory<Object, Object>
{

  @JsonCreator
  public DefaultAggregateAveragerFactory(
      @JsonProperty("name") String name,
      @JsonProperty("buckets") int numBuckets,
      @JsonProperty("cycleSize") Integer cycleSize,
      @JsonProperty("fieldName") String fieldName
  )
  {
    super(name, numBuckets, fieldName, cycleSize);
  }

  @Override
  public Averager<Object> createAverager()
  {
    throw new UnsupportedOperationException("Invalid operation for DefaultAggregateAveragerFactory.");
  }

  @Override
  public Averager<Object> createAverager(Map<String, AggregatorFactory> aggMap)
  {
    AggregatorFactory factory = aggMap.get(fieldName);
    Preconditions.checkArgument(factory != null, "Can't find the dependent field of DefaultAggregateAverager.");
    return new DefaultAggregateAverager(
        Object.class,
        numBuckets,
        name,
        fieldName,
        cycleSize,
        aggMap.get(fieldName)
    );
  }

  @Override
  public Comparator getComparator()
  {
    return Comparator.naturalOrder();
  }

}
