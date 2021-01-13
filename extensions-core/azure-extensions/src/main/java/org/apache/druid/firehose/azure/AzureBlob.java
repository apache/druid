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

package org.apache.druid.firehose.azure;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;

import javax.validation.constraints.NotNull;
import java.net.URI;
import java.util.Objects;

/**
 * Represents an Azure based blob. Used with {@link StaticAzureBlobStoreFirehoseFactory}.
 *
 * @deprecated as of version 0.18.0 because support for firehose has been discontinued. Please use
 * {@link org.apache.druid.data.input.azure.AzureEntity} with {@link org.apache.druid.data.input.azure.AzureInputSource}
 * instead.
 */
@Deprecated
public class AzureBlob
{
  @JsonProperty
  @NotNull
  private String container;

  @JsonProperty
  @NotNull
  private String path;

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

  public URI toURI()
  {
    return URI.create(StringUtils.format("azure://%s/%s", container, path));
  }
}
