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

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Objects;

public class PlannerOperatorConfig
{
  public static final String CONFIG_PATH = "druid.sql.planner.operator";
  @JsonProperty
  private List<String> denyList;


  @NotNull
  public List<String> getDenyList()
  {
    return denyList == null ? ImmutableList.of() : denyList;
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
    final PlannerOperatorConfig that = (PlannerOperatorConfig) o;
    return denyList.equals(that.denyList);
  }

  @Override
  public int hashCode()
  {

    return Objects.hash(denyList);
  }

  @Override
  public String toString()
  {
    return "PlannerOperatorConfig{" +
           "denyList=" + denyList +
           '}';
  }

  public static PlannerOperatorConfig newInstance(List<String> denyList)
  {
    PlannerOperatorConfig config = new PlannerOperatorConfig();
    config.denyList = denyList;
    return config;
  }
}
