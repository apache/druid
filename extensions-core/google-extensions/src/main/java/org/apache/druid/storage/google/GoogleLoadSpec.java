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

package org.apache.druid.storage.google;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.segment.loading.SegmentLoadingException;

import java.io.File;

@JsonTypeName(GoogleStorageDruidModule.SCHEME)
public class GoogleLoadSpec implements LoadSpec
{
  @JsonProperty
  private final String bucket;

  @JsonProperty
  private final String path;

  private final GoogleDataSegmentPuller puller;

  @JsonCreator
  public GoogleLoadSpec(
      @JsonProperty("bucket") String bucket,
      @JsonProperty("path") String path,
      @JacksonInject GoogleDataSegmentPuller puller
  )
  {
    Preconditions.checkNotNull(bucket);
    Preconditions.checkNotNull(path);
    this.bucket = bucket;
    this.path = path;
    this.puller = puller;
  }

  @Override
  public LoadSpecResult loadSegment(File file) throws SegmentLoadingException
  {
    return new LoadSpecResult(puller.getSegmentFiles(bucket, path, file).size());
  }
}
