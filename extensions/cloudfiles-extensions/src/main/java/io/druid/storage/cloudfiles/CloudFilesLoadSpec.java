/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.storage.cloudfiles;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.segment.loading.LoadSpec;
import io.druid.segment.loading.SegmentLoadingException;

import java.io.File;

@JsonTypeName(CloudFilesStorageDruidModule.SCHEME)
public class CloudFilesLoadSpec implements LoadSpec
{

  @JsonProperty
  private final String region;

  @JsonProperty
  private final String container;

  @JsonProperty
  private final String path;

  private final CloudFilesDataSegmentPuller puller;

  @JsonCreator
  public CloudFilesLoadSpec(
      @JsonProperty("region") String region, @JsonProperty("container") String container,
      @JsonProperty("path") String path, @JacksonInject CloudFilesDataSegmentPuller puller
  )
  {
    Preconditions.checkNotNull(region);
    Preconditions.checkNotNull(container);
    Preconditions.checkNotNull(path);
    this.container = container;
    this.region = region;
    this.path = path;
    this.puller = puller;
  }

  @Override
  public LoadSpecResult loadSegment(File file) throws SegmentLoadingException
  {
    return new LoadSpecResult(puller.getSegmentFiles(region, container, path, file).size());
  }
}
