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

package org.apache.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class EnvironmentVariableDynamicConfigProvider implements DynamicConfigProvider<String>
{
  private final ImmutableMap<String, String> variables;

  @JsonCreator
  public EnvironmentVariableDynamicConfigProvider(
      @JsonProperty("variables") Map<String, String> config
  )
  {
    this.variables = ImmutableMap.copyOf(Preconditions.checkNotNull(config, "config"));
  }

  @JsonProperty("variables")
  public Map<String, String> getVariables()
  {
    return variables;
  }

  @Override
  public Map<String, String> getConfig()
  {
    HashMap<String, String> map = new HashMap<>();
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      map.put(entry.getKey(), getEnv(entry.getValue()));
    }
    return map;
  }

  @VisibleForTesting
  protected String getEnv(String var)
  {
    return System.getenv(var);
  }

  @Override
  public String toString()
  {
    return "EnvironmentVariablePasswordProvider{" +
        "variable='" + variables + '\'' +
        '}';
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

    EnvironmentVariableDynamicConfigProvider that = (EnvironmentVariableDynamicConfigProvider) o;

    return Objects.equals(variables, that.variables);

  }

  @Override
  public int hashCode()
  {
    return variables.hashCode();
  }

}
