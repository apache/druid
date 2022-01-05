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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.query.filter.DimFilter;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * Spec containing transform configs for Compaction Task.
 * This class mimics JSON field names for fields supported in compaction task with
 * the corresponding fields in {@link org.apache.druid.segment.transform.TransformSpec}.
 * This is done for end-user ease of use. Basically, end-user will use the same syntax / JSON structure to set
 * transform configs for Compaction task as they would for any other ingestion task.
 */
public class ClientCompactionTaskTransformSpec
{
  @Nullable private final DimFilter filter;

  @JsonCreator
  public ClientCompactionTaskTransformSpec(
      @JsonProperty("filter") final DimFilter filter
  )
  {
    this.filter = filter;
  }

  @JsonProperty
  @Nullable
  public DimFilter getFilter()
  {
    return filter;
  }

  public Map<String, Object> asMap(ObjectMapper objectMapper)
  {
    return objectMapper.convertValue(
        this,
        new TypeReference<Map<String, Object>>() {}
    );
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
    ClientCompactionTaskTransformSpec that = (ClientCompactionTaskTransformSpec) o;
    return Objects.equals(filter, that.filter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(filter);
  }

  @Override
  public String toString()
  {
    return "ClientCompactionTaskTransformSpec{" +
           "filter=" + filter +
           '}';
  }
}
