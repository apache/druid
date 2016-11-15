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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Comparator;

public class TimestampMinAggregatorFactory extends TimestampAggregatorFactory
{
  @JsonCreator
  public TimestampMinAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("timeFormat") String timeFormat
  )
  {
    super(name, fieldName, timeFormat, new Comparator<Long>() {
      @Override
      public int compare(Long o1, Long o2) {
        return -(Long.compare(o1, o2));
      }
    }, Long.MAX_VALUE);
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");
  }

  @Override
  public String toString()
  {
    return "TimestampMinAggregatorFactory{" +
        "fieldName='" + fieldName + '\'' +
        ", name='" + name + '\'' +
        '}';
  }
}
