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

package io.druid.firehose.cloudfiles;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.Objects;

public class CloudFilesBlob
{
  @JsonProperty
  @NotNull
  private String container = null;

  @JsonProperty
  @NotNull
  private String path = null;

  @JsonProperty
  @NotNull
  private String region = null;

  public CloudFilesBlob()
  {
  }

  public CloudFilesBlob(String container, String path, String region)
  {
    this.container = container;
    this.path = path;
    this.region = region;
  }

  public String getContainer()
  {
    return container;
  }

  public String getPath()
  {
    return path;
  }

  public String getRegion()
  {
    return region;
  }

  @Override
  public String toString()
  {
    return "CloudFilesBlob{"
        + "container=" + container
        + ",path=" + path
        + ",region=" + region
        + "}";
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

    final CloudFilesBlob that = (CloudFilesBlob) o;
    return Objects.equals(container, that.container) &&
           Objects.equals(path, that.path) &&
           Objects.equals(region, that.region);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(container, path, region);
  }
}
