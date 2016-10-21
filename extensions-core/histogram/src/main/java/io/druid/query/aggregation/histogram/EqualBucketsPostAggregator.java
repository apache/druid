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

package io.druid.query.aggregation.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Sets;

import io.druid.java.util.common.IAE;

import java.util.Map;
import java.util.Set;

@JsonTypeName("equalBuckets")
public class EqualBucketsPostAggregator extends ApproximateHistogramPostAggregator
{
  private final int numBuckets;
  private String fieldName;

  @JsonCreator
  public EqualBucketsPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("numBuckets") int numBuckets
  )
  {
    super(name, fieldName);
    this.numBuckets = numBuckets;
    if (this.numBuckets <= 1) {
      throw new IAE("Illegal number of buckets[%s], must be > 1", this.numBuckets);
    }
    this.fieldName = fieldName;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Object compute(Map<String, Object> values)
  {
    ApproximateHistogram ah = (ApproximateHistogram) values.get(this.getFieldName());
    return ah.toHistogram(numBuckets);
  }

  @JsonProperty
  public int getNumBuckets()
  {
    return numBuckets;
  }

  @Override
  public String toString()
  {
    return "EqualBucketsPostAggregator{" +
           "name='" + this.getName() + '\'' +
           ", fieldName='" + this.getFieldName() + '\'' +
           ", numBuckets=" + this.getNumBuckets() +
           '}';
  }
}
