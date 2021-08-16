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

package org.apache.druid.server.coordinator.duty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Duration;

import java.util.List;
import java.util.Objects;

public class CoordinatorCustomDutyGroup
{
  @JsonProperty
  private String name;

  @JsonProperty
  private Duration period;

  @JsonProperty
  private List<CoordinatorCustomDuty> customDutyList;

  @JsonCreator
  public CoordinatorCustomDutyGroup(
      @JsonProperty("name") String name,
      @JsonProperty("period") Duration period,
      @JsonProperty("customDutyList") List<CoordinatorCustomDuty> customDutyList
  )
  {
    //TODO add check
    this.name = name;
    this.period = period;
    this.customDutyList = customDutyList;
  }

  public String getName()
  {
    return name;
  }

  public Duration getPeriod()
  {
    return period;
  }

  public List<CoordinatorCustomDuty> getCustomDutyList()
  {
    return customDutyList;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CoordinatorCustomDutyGroup that = (CoordinatorCustomDutyGroup) o;
    return Objects.equals(name, that.name) &&
           Objects.equals(period, that.period) &&
           Objects.equals(customDutyList, that.customDutyList);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, period, customDutyList);
  }

  @Override
  public String toString()
  {
    return "CoordinatorCustomDutyGroup{" +
           "name='" + name + '\'' +
           ", period=" + period +
           ", customDutyList=" + customDutyList +
           '}';
  }
}
