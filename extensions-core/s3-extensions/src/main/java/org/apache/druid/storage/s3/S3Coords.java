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

package org.apache.druid.storage.s3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.net.URI;
import java.util.Objects;

public class S3Coords
{
  final String bucket;
  final String path;

  public S3Coords(URI uri)
  {
    if (!"s3".equalsIgnoreCase(uri.getScheme())) {
      throw new IAE("Unsupported scheme: [%s]", uri.getScheme());
    }
    bucket = uri.getHost();
    String path = uri.getPath();
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    this.path = path;
  }

  @JsonCreator
  public S3Coords(@JsonProperty("bucket") String bucket, @JsonProperty("path") String key)
  {
    this.bucket = bucket;
    this.path = key;
  }

  @JsonProperty("bucket")
  public String getBucket()
  {
    return bucket;
  }

  @JsonProperty("path")
  public String getPath()
  {
    return path;
  }

  @Override
  public String toString()
  {
    return StringUtils.format("s3://%s/%s", bucket, path);
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
    S3Coords s3Coords = (S3Coords) o;
    return bucket.equals(s3Coords.bucket) &&
           path.equals(s3Coords.path);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(bucket, path);
  }
}
