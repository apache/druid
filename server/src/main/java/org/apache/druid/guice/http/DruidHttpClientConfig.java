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

package org.apache.druid.guice.http;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.JvmUtils;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.validation.constraints.Min;

/**
 *
 */

public class DruidHttpClientConfig
{
  private static final String DEFAULT_COMPRESSION_CODEC = "gzip";
  private static final double DEFAULT_MAX_QUEUED_BYTES_HEAP_FRACTION = 0.02; // Per query, so 2% is reasonably safe
  private static final Logger LOG = new Logger(DruidHttpClientConfig.class);

  @JsonProperty
  @Min(0)
  private int numConnections = 20;

  @JsonProperty
  private Period readTimeout = new Period("PT15M");

  @JsonProperty
  @Min(1)
  private int numMaxThreads = Math.max(10, (JvmUtils.getRuntimeInfo().getAvailableProcessors() * 17) / 16 + 2) + 30;

  @JsonProperty
  @Min(1)
  private int numRequestsQueued = 1024;

  @JsonProperty
  private String compressionCodec = DEFAULT_COMPRESSION_CODEC;

  @JsonProperty
  private int requestBuffersize = 8 * 1024;

  @JsonProperty
  private Period unusedConnectionTimeout = new Period("PT4M");

  /**
   * Maximum number of bytes queued per query before exerting backpressure. Not always used; currently, it's only
   * respected by CachingClusteredClient (broker -> data server communication).
   */
  @JsonProperty
  private HumanReadableBytes maxQueuedBytes = computeDefaultMaxQueuedBytes();

  @JsonProperty
  private Boolean eagerInitialization = null;

  public int getNumConnections()
  {
    return numConnections;
  }

  public Duration getReadTimeout()
  {
    return readTimeout == null ? null : readTimeout.toStandardDuration();
  }

  public int getNumMaxThreads()
  {
    return numMaxThreads;
  }

  public String getCompressionCodec()
  {
    return compressionCodec;
  }

  public int getNumRequestsQueued()
  {
    return numRequestsQueued;
  }

  public int getRequestBuffersize()
  {
    return requestBuffersize;
  }

  public Duration getUnusedConnectionTimeout()
  {
    if (unusedConnectionTimeout != null && readTimeout != null
        && unusedConnectionTimeout.toStandardDuration().compareTo(readTimeout.toStandardDuration()) >= 0) {
      LOG.warn(
          "Ohh no! UnusedConnectionTimeout[%s] is longer than readTimeout[%s], please correct"
          + " the configuration, this might not be supported in future.",
          unusedConnectionTimeout,
          readTimeout
      );
    }
    return unusedConnectionTimeout == null ? null : unusedConnectionTimeout.toStandardDuration();
  }

  public long getMaxQueuedBytes()
  {
    return maxQueuedBytes.getBytes();
  }

  public boolean isEagerInitialization(boolean defaultValue)
  {
    if (null == eagerInitialization) {
      return defaultValue;
    }
    return eagerInitialization;
  }

  private static HumanReadableBytes computeDefaultMaxQueuedBytes()
  {
    return HumanReadableBytes.valueOf(
        Math.max(
            25_000_000,
            (long) (JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes() * DEFAULT_MAX_QUEUED_BYTES_HEAP_FRACTION)
        )
    );
  }
}
