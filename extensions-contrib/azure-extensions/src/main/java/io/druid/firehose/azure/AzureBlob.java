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

package io.druid.firehose.azure;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.Objects;


public class AzureBlob
{
  @JsonProperty
  @NotNull
  private String container = null;

  @JsonProperty
  @NotNull
  private String path = null;

  @JsonCreator
  public AzureBlob(@JsonProperty("container") String container, @JsonProperty("path") String path)
  {
    this.container = container;
    this.path = path;
  }

  public String getContainer()
  {
    return container;
  }

  public String getPath()
  {
    return path;
  }

  @Override
  public String toString()
  {
    return "AzureBlob{"
        + "container=" + container
        + ",path=" + path
        + "}";
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AzureBlob that = (AzureBlob) o;
    return Objects.equals(container, that.container) &&
           Objects.equals(path, that.path);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(container, path);
  }
}
