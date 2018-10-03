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
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

public class JSONPathSpec
{
  public static final JSONPathSpec DEFAULT = new JSONPathSpec(null, null);

  private final boolean useFieldDiscovery;
  private final List<JSONPathFieldSpec> fields;

  @JsonCreator
  public JSONPathSpec(
      @JsonProperty("useFieldDiscovery") Boolean useFieldDiscovery,
      @JsonProperty("fields") List<JSONPathFieldSpec> fields
  )
  {
    this.useFieldDiscovery = useFieldDiscovery == null ? true : useFieldDiscovery;
    this.fields = fields == null ? ImmutableList.of() : fields;
  }

  @JsonProperty
  public boolean isUseFieldDiscovery()
  {
    return useFieldDiscovery;
  }

  @JsonProperty
  public List<JSONPathFieldSpec> getFields()
  {
    return fields;
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
    final JSONPathSpec that = (JSONPathSpec) o;
    return useFieldDiscovery == that.useFieldDiscovery &&
           Objects.equals(fields, that.fields);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(useFieldDiscovery, fields);
  }

  @Override
  public String toString()
  {
    return "JSONPathSpec{" +
           "useFieldDiscovery=" + useFieldDiscovery +
           ", fields=" + fields +
           '}';
  }
}
