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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;

import java.net.URI;
import java.util.Objects;

public class CloudObjectLocation
{
  private final String bucket;
  private final String path;

  @JsonCreator
  public CloudObjectLocation(@JsonProperty("bucket") String bucket, @JsonProperty("path") String path)
  {
    this.bucket = Preconditions.checkNotNull(bucket);
    this.path = Preconditions.checkNotNull(path);
  }

  public CloudObjectLocation(URI uri)
  {
    bucket = uri.getHost();
    String path = StringUtils.maybeRemoveLeadingSlash(uri.getPath());
    this.path = path;
  }

  @JsonProperty
  public String getBucket()
  {
    return bucket;
  }

  @JsonProperty
  public String getPath()
  {
    return path;
  }

  @Override
  public String toString()
  {
    return "CloudObjectLocation {"
           + "bucket=" + bucket
           + ",path=" + path
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

    final CloudObjectLocation that = (CloudObjectLocation) o;
    return Objects.equals(bucket, that.bucket) &&
           Objects.equals(path, that.path);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(bucket, path);
  }
}
