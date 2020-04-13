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
import com.fasterxml.jackson.annotation.JsonProperty;

public class Resource
{
  public static final Resource STATE_RESOURCE = new Resource("STATE", ResourceType.STATE);

  private final String name;
  private final ResourceType type;

  @JsonCreator
  public Resource(
      @JsonProperty("name") String name,
      @JsonProperty("type") ResourceType type
  )
  {
    this.name = name;
    this.type = type;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public ResourceType getType()
  {
    return type;
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

    Resource resource = (Resource) o;

    if (!name.equals(resource.name)) {
      return false;
    }
    return type == resource.type;

  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + type.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "Resource{" +
           "name='" + name + '\'' +
           ", type=" + type +
           '}';
  }
}
