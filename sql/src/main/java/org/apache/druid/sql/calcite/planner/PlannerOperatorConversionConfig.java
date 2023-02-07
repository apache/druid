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

package org.apache.druid.sql.calcite.planner;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

public class PlannerOperatorConversionConfig
{
  private static final List<String> DEFAULT_DENY_LIST = ImmutableList.of();
  @JsonProperty
  private List<String> denyList = DEFAULT_DENY_LIST;


  public List<String> getDenyList()
  {
    return denyList;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PlannerOperatorConversionConfig that = (PlannerOperatorConversionConfig) o;
    return denyList.equals(that.denyList);
  }

  @Override
  public int hashCode()
  {

    return Objects.hash(
        denyList
    );
  }

  @Override
  public String toString()
  {
    return "PlannerOperatorConversionConfig{" +
           "denyList=" + denyList +
           '}';
  }

  public static Builder builder()
  {
    return new PlannerOperatorConversionConfig().toBuilder();
  }

  public Builder toBuilder()
  {
    return new Builder(this);
  }

  /**
   * Builder for {@link PlannerConfig}, primarily for use in tests to
   * allow setting options programmatically rather than from the command
   * line or a properties file. Starts with values from an existing
   * (typically default) config.
   */
  public static class Builder
  {
    private List<String> denyList = DEFAULT_DENY_LIST;

    public Builder(PlannerOperatorConversionConfig base)
    {
      this.denyList = base.denyList;
    }

    public Builder denyList(List<String> denyList)
    {
      this.denyList = denyList;
      return this;
    }

    public PlannerOperatorConversionConfig build()
    {
      PlannerOperatorConversionConfig config = new PlannerOperatorConversionConfig();
      config.denyList = denyList;
      return config;
    }
  }
}
