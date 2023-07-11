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

package org.apache.druid.segment.nested;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.Objects;


public class NestedPathField implements NestedPathPart
{
  private final String field;

  @JsonCreator
  public NestedPathField(@JsonProperty("field") String field)
  {
    this.field = Preconditions.checkNotNull(field, "partIdentifier must not be null");
  }

  @Override
  public Object find(Object input)
  {
    if (input instanceof Map) {
      Map<String, Object> currentMap = (Map<String, Object>) input;
      return currentMap.get(field);
    }
    return null;
  }

  @Override
  public String getPartIdentifier()
  {
    return field;
  }

  @JsonProperty("field")
  public String getField()
  {
    return field;
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
    NestedPathField that = (NestedPathField) o;
    return field.equals(that.field);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(field);
  }

  @Override
  public String toString()
  {
    return "NestedPathField{" +
           "field='" + field + '\'' +
           '}';
  }
}
