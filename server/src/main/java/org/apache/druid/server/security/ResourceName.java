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

package org.apache.druid.server.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.druid.java.util.common.StringUtils;

import javax.validation.constraints.NotNull;
import java.util.Objects;

public class ResourceName
{
  public static final ResourceName INTERNAL = new ResourceName("INTERNAL");
  public static final ResourceName LOOKUP = new ResourceName("LOOKUP");
  public static final ResourceName SERVER = new ResourceName("SERVER");
  public static final ResourceName STATUS = new ResourceName("STATUS");

  private final String resourceName;

  public ResourceName(String name)
  {
    this.resourceName = StringUtils.toUpperCase(name);
  }

  @JsonCreator
  public static ResourceName fromString(@NotNull String name)
  {
    return new ResourceName(StringUtils.toUpperCase(name));
  }

  @Override
  public String toString()
  {
    return this.resourceName;
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
    ResourceName that = (ResourceName) o;
    return resourceName.equals(that.resourceName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(resourceName);
  }
}
