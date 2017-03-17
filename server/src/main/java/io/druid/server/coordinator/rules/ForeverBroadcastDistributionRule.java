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

package io.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Objects;

public class ForeverBroadcastDistributionRule extends BroadcastDistributionRule
{
  static final String TYPE = "broadcastForever";

  private final String colocateDataSource;

  @JsonCreator
  public ForeverBroadcastDistributionRule(
      @JsonProperty("colocateDataSource") String colocateDataSource
  )
  {
    this.colocateDataSource = Objects.requireNonNull(colocateDataSource);
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
  {
    return true;
  }

  @Override
  public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
  {
    return true;
  }

  @Override
  @JsonProperty
  public String getColocateDataSource()
  {
    return colocateDataSource;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }

    if (o.getClass() == ForeverBroadcastDistributionRule.class) {
      ForeverBroadcastDistributionRule that = (ForeverBroadcastDistributionRule) o;
      return colocateDataSource.equals(that.colocateDataSource);
    }

    return false;
  }
}
