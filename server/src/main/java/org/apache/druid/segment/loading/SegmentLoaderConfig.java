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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.com.google.common.annotations.VisibleForTesting;
import org.apache.druid.com.google.common.collect.Lists;
import org.apache.druid.utils.JvmUtils;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 *
 */
public class SegmentLoaderConfig
{
  @JsonProperty
  private List<StorageLocationConfig> locations = Collections.emptyList();

  @JsonProperty("lazyLoadOnStart")
  private boolean lazyLoadOnStart = false;

  @JsonProperty("deleteOnRemove")
  private boolean deleteOnRemove = true;

  @JsonProperty("dropSegmentDelayMillis")
  private int dropSegmentDelayMillis = (int) TimeUnit.SECONDS.toMillis(30);

  @JsonProperty("announceIntervalMillis")
  private int announceIntervalMillis = 0; // do not background announce

  @JsonProperty("numLoadingThreads")
  private int numLoadingThreads = Math.max(1, JvmUtils.getRuntimeInfo().getAvailableProcessors() / 6);

  @JsonProperty("numBootstrapThreads")
  private Integer numBootstrapThreads = null;

  @JsonProperty
  private File infoDir = null;

  @JsonProperty
  private int statusQueueMaxSize = 100;

  private long combinedMaxSize = 0;

  public List<StorageLocationConfig> getLocations()
  {
    return locations;
  }

  public boolean isLazyLoadOnStart()
  {
    return lazyLoadOnStart;
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

  public int getStatusQueueMaxSize()
  {
    return statusQueueMaxSize;
  }

  public long getCombinedMaxSize()
  {
    if (combinedMaxSize == 0) {
      combinedMaxSize = getLocations().stream().mapToLong(StorageLocationConfig::getMaxSize).sum();
    }
    return combinedMaxSize;
  }

  public SegmentLoaderConfig withLocations(List<StorageLocationConfig> locations)
  {
    SegmentLoaderConfig retVal = new SegmentLoaderConfig();
    retVal.locations = Lists.newArrayList(locations);
    retVal.deleteOnRemove = this.deleteOnRemove;
    retVal.infoDir = this.infoDir;
    return retVal;
  }

  @VisibleForTesting
  public SegmentLoaderConfig withInfoDir(File infoDir)
  {
    SegmentLoaderConfig retVal = new SegmentLoaderConfig();
    retVal.locations = this.locations;
    retVal.deleteOnRemove = this.deleteOnRemove;
    retVal.infoDir = infoDir;
    return retVal;
  }

  /**
   * Convert StorageLocationConfig objects to StorageLocation objects
   * <p>
   * Note: {@link #getLocations} is called instead of variable access because some testcases overrides this method
   */
  public List<StorageLocation> toStorageLocations()
  {
    return this.getLocations()
               .stream()
               .map(locationConfig -> new StorageLocation(locationConfig.getPath(),
                                                          locationConfig.getMaxSize(),
                                                          locationConfig.getFreeSpacePercent()))
               .collect(Collectors.toList());
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
