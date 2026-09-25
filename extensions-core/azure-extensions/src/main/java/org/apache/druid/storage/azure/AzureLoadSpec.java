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

package org.apache.druid.storage.azure;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentRangeReader;
import org.apache.druid.utils.CompressionUtils;

import javax.annotation.Nullable;
import java.io.File;

/**
 * A means of pulling segment files into Azure based deep storage
 */
@JsonTypeName(AzureStorageDruidModule.SCHEME)
public class AzureLoadSpec implements LoadSpec
{
  static final String RANGEABLE = "rangeable";

  @JsonProperty
  private final String containerName;

  @JsonProperty
  private final String blobPath;

  /**
   * Stamped at push time when {@link AzureDataSegmentPusher} writes a segment in a layout that supports byte-range
   * reads. Only {@code Boolean.TRUE} enables {@link #openRangeReader()}; absence or {@code false} means full-download.
   */
  @Nullable
  private final Boolean rangeable;

  private final AzureDataSegmentPuller puller;

  @JsonCreator
  public AzureLoadSpec(
      @JsonProperty("containerName") String containerName,
      @JsonProperty("blobPath") String blobPath,
      @JsonProperty(RANGEABLE) @Nullable Boolean rangeable,
      @JacksonInject AzureDataSegmentPuller puller
  )
  {
    Preconditions.checkNotNull(blobPath);
    Preconditions.checkNotNull(containerName);
    this.containerName = containerName;
    this.blobPath = blobPath;
    this.rangeable = rangeable;
    this.puller = puller;
  }

  @Override
  public LoadSpecResult loadSegment(File file) throws SegmentLoadingException
  {
    return new LoadSpecResult(puller.getSegmentFiles(containerName, blobPath, file).size());
  }

  /**
   * Returns an {@link AzureSegmentRangeReader} when the segment was stamped {@link #rangeable} {@code true} at push
   * time and isn't a zip; otherwise {@code null}.
   */
  @Nullable
  @Override
  public SegmentRangeReader openRangeReader()
  {
    if (CompressionUtils.isZip(blobPath) || !Boolean.TRUE.equals(rangeable)) {
      return null;
    }
    return new AzureSegmentRangeReader(
        puller.getAzureStorage(),
        containerName,
        blobPath,
        puller.getAccountConfig().getMaxTries()
    );
  }

  /**
   * Returns the range-reads-supported flag stamped at push time, or {@code null} for legacy segments pushed before
   * this field existed (which will load via the full-download path).
   */
  @JsonProperty(RANGEABLE)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public Boolean getRangeable()
  {
    return rangeable;
  }
}
