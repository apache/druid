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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class EnvironmentVariablePasswordProvider implements PasswordProvider
{
  private final String variable;

  @JsonCreator
  public EnvironmentVariablePasswordProvider(
      @JsonProperty("variable") String variable
  )
  {
    this.variable = Preconditions.checkNotNull(variable);
  }

  @JsonProperty("variable")
  public String getVariable()
  {
    return variable;
  }

  @JsonIgnore
  @Override
  public String getPassword()
  {
    return System.getenv(variable);
  }

  @Override
  public String toString()
  {
    return "EnvironmentVariablePasswordProvider{" +
           "variable='" + variable + '\'' +
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

    EnvironmentVariablePasswordProvider that = (EnvironmentVariablePasswordProvider) o;

    return variable != null ? variable.equals(that.variable) : that.variable == null;

  }

  @Override
  public int hashCode()
  {
    return variable != null ? variable.hashCode() : 0;
  }
}
