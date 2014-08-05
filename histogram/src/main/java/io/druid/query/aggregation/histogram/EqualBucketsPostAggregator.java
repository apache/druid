/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Sets;
import com.metamx.common.IAE;

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
