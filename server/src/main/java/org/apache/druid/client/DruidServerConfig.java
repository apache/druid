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

package org.apache.druid.client;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import org.apache.druid.com.google.common.collect.Sets;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.HumanReadableBytesRange;
import org.apache.druid.segment.loading.SegmentLoaderConfig;

import javax.validation.constraints.NotNull;
import java.util.Set;

/**
 */
public class DruidServerConfig
{
  @JsonProperty
  @HumanReadableBytesRange(min = 0)
  private HumanReadableBytes maxSize = HumanReadableBytes.ZERO;

  @JsonProperty
  private String tier = DruidServer.DEFAULT_TIER;

  @JsonProperty
  private int priority = DruidServer.DEFAULT_PRIORITY;

  @JsonProperty
  @NotNull
  private Set<String> hiddenProperties = Sets.newHashSet("druid.s3.accessKey", "druid.s3.secretKey", "druid.metadata.storage.connector.password");

  private SegmentLoaderConfig segmentLoaderConfig;

  // Guice inject added here to properly bind this dependency into its dependents such as StatusResource
  @Inject
  @JsonCreator
  public DruidServerConfig(
      @JacksonInject SegmentLoaderConfig segmentLoaderConfig
  )
  {
    this.segmentLoaderConfig = segmentLoaderConfig;
  }

  public long getMaxSize()
  {
    if (maxSize.equals(HumanReadableBytes.ZERO)) {
      return segmentLoaderConfig.getCombinedMaxSize();
    }
    return maxSize.getBytes();
  }

  public String getTier()
  {
    return tier;
  }

  public int getPriority()
  {
    return priority;
  }

  public Set<String> getHiddenProperties()
  {
    return hiddenProperties;
  }

}
