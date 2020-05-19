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

package org.apache.druid.query.aggregation.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

@JsonTypeName("min")
public class MinPostAggregator extends ApproximateHistogramPostAggregator
{
  // this doesn't need to handle nulls because the values come from ApproximateHistogram
  static final Comparator COMPARATOR = Comparator.comparingDouble(o -> ((Number) o).doubleValue());

  @JsonCreator
  public MinPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    super(name, fieldName);
  }

  @Override
  public Comparator getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Object compute(Map<String, Object> values)
  {
    Object val = values.get(fieldName);
    if (val instanceof ApproximateHistogram) {
      final ApproximateHistogram ah = (ApproximateHistogram) val;
      return ah.getMin();
    } else if (val instanceof FixedBucketsHistogram) {
      final FixedBucketsHistogram fbh = (FixedBucketsHistogram) val;
      return fbh.getMin();
    }
    throw new ISE("Unknown value type: " + val.getClass());
  }

  @Override
  public PostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return this;
  }

  @Override
  public String toString()
  {
    return "MinPostAggregator{" +
           "fieldName='" + fieldName + '\'' +
           '}';
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(PostAggregatorIds.HISTOGRAM_MIN)
        .appendString(fieldName)
        .build();
  }
}
