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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import javax.annotation.Nullable;
import java.util.List;

public class TimestampMaxAggregatorFactory extends TimestampAggregatorFactory
{
  @JsonCreator
  public TimestampMaxAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") @Nullable String fieldName,
      @JsonProperty("timeFormat") @Nullable String timeFormat
  )
  {
    super(name, fieldName, timeFormat, Ordering.natural(), Long.MIN_VALUE);
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new TimestampMaxAggregatorFactory(name, name, timeFormat);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return ImmutableList.of(
        new TimestampMaxAggregatorFactory(name, fieldName, timeFormat)
    );
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new TimestampMaxAggregatorFactory(newName, getFieldName(), getTimeFormat());
  }

  @Override
  public String toString()
  {
    return "TimestampMaxAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", timeFormat='" + timeFormat + '\'' +
           '}';
  }
}
