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

package org.apache.druid.java.util.common.parsers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class JSONExplodeSpec
{
  public static final JSONExplodeSpec DEFAULT =
      new JSONExplodeSpec(null, null);

  private final String path;
  private final String type;

  @JsonCreator
  public JSONExplodeSpec(
      @JsonProperty("path") String path,
      @JsonProperty("type") String type
  )
  {
    this.path = path;
    this.type = type;
  }

  @JsonProperty
  public String getPath()
  {
    return path;
  }

  @JsonProperty
  public String getType()
  {
    return type;
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
    final JSONExplodeSpec that = (JSONExplodeSpec) o;
    return path.equals(that.path) &&
           type.equals(that.type);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(path, type);
  }

  @Override
  public String toString()
  {
    return "JSONExplodeSpec{" +
           "path=" + path +
           ", type=" + type +
           '}';
  }
}

