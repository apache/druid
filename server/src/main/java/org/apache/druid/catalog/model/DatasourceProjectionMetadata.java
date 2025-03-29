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

package org.apache.druid.catalog.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;

import java.util.Objects;

/**
 * Catalog model wrapper for projection spec
 */
public class DatasourceProjectionMetadata
{
  private final AggregateProjectionSpec spec;

  @JsonCreator
  public DatasourceProjectionMetadata(
      @JsonProperty("spec") AggregateProjectionSpec spec
  )
  {
    this.spec = spec;
  }

  @JsonProperty
  public AggregateProjectionSpec getSpec()
  {
    return spec;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatasourceProjectionMetadata that = (DatasourceProjectionMetadata) o;
    return Objects.equals(spec, that.spec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(spec);
  }

  @Override
  public String toString()
  {
    return "DatasourceProjectionMetadata{" +
           "spec=" + spec +
           '}';
  }
}
