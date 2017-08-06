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

package io.druid.firehose.google;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class GoogleBlob
{
  private final String bucket;
  private final String path;

  @JsonCreator
  public GoogleBlob(@JsonProperty("bucket") String bucket, @JsonProperty("path") String path)
  {
    this.bucket = bucket;
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
    return "GoogleBlob {"
        + "bucket=" + bucket
        + ",path=" + path
        + "}";
  }
}

