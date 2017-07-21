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

package io.druid.segment.loading;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.File;
import java.util.List;

/**
 */
public class SegmentLoaderConfig
{
  @JsonProperty
  @NotEmpty
  private List<StorageLocationConfig> locations = null;

  @JsonProperty("deleteOnRemove")
  private boolean deleteOnRemove = true;

  @JsonProperty("dropSegmentDelayMillis")
  private int dropSegmentDelayMillis = 30 * 1000; // 30 seconds

  @JsonProperty("announceIntervalMillis")
  private int announceIntervalMillis = 0; // do not background announce

  @JsonProperty("numLoadingThreads")
  private int numLoadingThreads = 1;

  @JsonProperty("numBootstrapThreads")
  private Integer numBootstrapThreads = null;

  @JsonProperty
  private File infoDir = null;

  public List<StorageLocationConfig> getLocations()
  {
    return locations;
  }

  public boolean isDeleteOnRemove()
  {
    return deleteOnRemove;
  }

  public int getDropSegmentDelayMillis()
  {
    return dropSegmentDelayMillis;
  }

  public int getAnnounceIntervalMillis()
  {
    return announceIntervalMillis;
  }

  public int getNumLoadingThreads()
  {
    return numLoadingThreads;
  }

  public int getNumBootstrapThreads()
  {
    return numBootstrapThreads == null ? numLoadingThreads : numBootstrapThreads;
  }

  public File getInfoDir()
  {
    if (infoDir == null) {
      infoDir = new File(locations.get(0).getPath(), "info_dir");
    }

    return infoDir;
  }

  public SegmentLoaderConfig withLocations(List<StorageLocationConfig> locations)
  {
    SegmentLoaderConfig retVal = new SegmentLoaderConfig();
    retVal.locations = Lists.newArrayList(locations);
    retVal.deleteOnRemove = this.deleteOnRemove;
    retVal.infoDir = this.infoDir;
    return retVal;
  }

  @Override
  public String toString()
  {
    return "SegmentLoaderConfig{" +
           "locations=" + locations +
           ", deleteOnRemove=" + deleteOnRemove +
           ", dropSegmentDelayMillis=" + dropSegmentDelayMillis +
           ", infoDir=" + infoDir +
           '}';
  }
}
