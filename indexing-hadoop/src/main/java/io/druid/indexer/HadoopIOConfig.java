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

package io.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.indexer.updater.MetadataStorageUpdaterJobSpec;
import io.druid.segment.indexing.IOConfig;

import java.util.Map;

/**
 */
@JsonTypeName("hadoop")
public class HadoopIOConfig implements IOConfig
{
  private final Map<String, Object> pathSpec;
  private final MetadataStorageUpdaterJobSpec metadataUpdateSpec;
  private final String segmentOutputPath;

  @JsonCreator
  public HadoopIOConfig(
      final @JsonProperty("inputSpec") Map<String, Object> pathSpec,
      final @JsonProperty("metadataUpdateSpec") MetadataStorageUpdaterJobSpec metadataUpdateSpec,
      final @JsonProperty("segmentOutputPath") String segmentOutputPath
  )
  {
    this.pathSpec = pathSpec;
    this.metadataUpdateSpec = metadataUpdateSpec;
    this.segmentOutputPath = segmentOutputPath;
  }

  @JsonProperty("inputSpec")
  public Map<String, Object> getPathSpec()
  {
    return pathSpec;
  }

  @JsonProperty("metadataUpdateSpec")
  public MetadataStorageUpdaterJobSpec getMetadataUpdateSpec()
  {
    return metadataUpdateSpec;
  }

  @JsonProperty
  public String getSegmentOutputPath()
  {
    return segmentOutputPath;
  }

  public HadoopIOConfig withSegmentOutputPath(String path)
  {
    return new HadoopIOConfig(pathSpec, metadataUpdateSpec, path);
  }
}
