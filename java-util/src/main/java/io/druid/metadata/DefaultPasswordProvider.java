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

package io.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DefaultPasswordProvider implements PasswordProvider
{
  private final String password;

  @JsonCreator
  public static DefaultPasswordProvider fromString(String str)
  {
    return new DefaultPasswordProvider(str);
  }

  @JsonCreator
  public DefaultPasswordProvider(@JsonProperty("password") String password)
  {
    this.password = password;
  }

  @Override
  @JsonProperty
  public String getPassword()
  {
    return password;
  }

  @Override
  public String toString()
  {
    return this.getClass().getCanonicalName();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DefaultPasswordProvider)) {
      return false;
    }

    DefaultPasswordProvider that = (DefaultPasswordProvider) o;

    return getPassword() != null ? getPassword().equals(that.getPassword()) : that.getPassword() == null;
  }

  @Override
  public int hashCode()
  {
    return getPassword() != null ? getPassword().hashCode() : 0;
  }
}

