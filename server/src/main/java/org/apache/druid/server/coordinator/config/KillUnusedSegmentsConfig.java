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

package org.apache.druid.server.coordinator.config;

import org.apache.druid.common.config.Configs;
import org.joda.time.Duration;

public class KillUnusedSegmentsConfig extends MetadataCleanupConfig
{
  private final int maxSegments;
  private final boolean ignoreDurationToRetain;
  private final Duration bufferPeriod;

  public KillUnusedSegmentsConfig(
      Boolean cleanupEnabled,
      Duration cleanupPeriod,
      Duration durationToRetain,
      Boolean ignoreDurationToRetain,
      Duration bufferPeriod,
      Integer maxSegments
  )
  {
    super(
        Configs.valueOrDefault(cleanupEnabled, false),
        cleanupPeriod,
        durationToRetain
    );
    this.ignoreDurationToRetain = Configs.valueOrDefault(ignoreDurationToRetain, false);
    this.bufferPeriod = Configs.valueOrDefault(bufferPeriod, Duration.standardDays(30));
    this.maxSegments = Configs.valueOrDefault(maxSegments, 100);
  }

  public int getMaxSegments()
  {
    return maxSegments;
  }

  public Duration getBufferPeriod()
  {
    return bufferPeriod;
  }

  public boolean isIgnoreDurationToRetain()
  {
    return ignoreDurationToRetain;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static class Builder
  {
    private Duration durationToRetain;
    private Duration cleanupPeriod;
    private Boolean ignoreDurationToRetain;
    private Duration bufferPeriod;
    private Integer maxSegments;

    private Builder()
    {

    }

    public KillUnusedSegmentsConfig build()
    {
      return new KillUnusedSegmentsConfig(
          true,
          cleanupPeriod,
          durationToRetain,
          ignoreDurationToRetain,
          bufferPeriod,
          maxSegments
      );
    }

    public Builder withCleanupPeriod(Duration cleanupPeriod)
    {
      this.cleanupPeriod = cleanupPeriod;
      return this;
    }

    public Builder withDurationToRetain(Duration durationToRetain)
    {
      this.durationToRetain = durationToRetain;
      return this;
    }

    public Builder withIgnoreDurationToRetain(Boolean ignoreDurationToRetain)
    {
      this.ignoreDurationToRetain = ignoreDurationToRetain;
      return this;
    }

    public Builder withMaxSegmentsToKill(Integer maxSegments)
    {
      this.maxSegments = maxSegments;
      return this;
    }

    public Builder withBufferPeriod(Duration bufferPeriod)
    {
      this.bufferPeriod = bufferPeriod;
      return this;
    }
  }
}
