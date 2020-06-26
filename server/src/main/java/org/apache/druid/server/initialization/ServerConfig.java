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

package org.apache.druid.server.initialization;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.druid.utils.JvmUtils;
import org.joda.time.Period;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;

/**
 *
 */
public class ServerConfig
{
  public static final int DEFAULT_GZIP_INFLATE_BUFFER_SIZE = 4096;

  /**
   * The ServerConfig is normally created using {@link org.apache.druid.guice.JsonConfigProvider} binding.
   *
   * This constructor is provided for callers that need to create a ServerConfig object with specific field values.
   */
  public ServerConfig(
      int numThreads,
      int queueSize,
      boolean enableRequestLimit,
      @NotNull Period maxIdleTime,
      long defaultQueryTimeout,
      long maxScatterGatherBytes,
      int maxSubqueryRows,
      long maxQueryTimeout,
      int maxRequestHeaderSize,
      @NotNull Period gracefulShutdownTimeout,
      @NotNull Period unannouncePropagationDelay,
      int inflateBufferSize,
      int compressionLevel,
      boolean enableForwardedRequestCustomizer,
      @NotNull List<String> allowedHttpMethods
  )
  {
    this.numThreads = numThreads;
    this.queueSize = queueSize;
    this.enableRequestLimit = enableRequestLimit;
    this.maxIdleTime = maxIdleTime;
    this.defaultQueryTimeout = defaultQueryTimeout;
    this.maxScatterGatherBytes = maxScatterGatherBytes;
    this.maxSubqueryRows = maxSubqueryRows;
    this.maxQueryTimeout = maxQueryTimeout;
    this.maxRequestHeaderSize = maxRequestHeaderSize;
    this.gracefulShutdownTimeout = gracefulShutdownTimeout;
    this.unannouncePropagationDelay = unannouncePropagationDelay;
    this.inflateBufferSize = inflateBufferSize;
    this.compressionLevel = compressionLevel;
    this.enableForwardedRequestCustomizer = enableForwardedRequestCustomizer;
    this.allowedHttpMethods = allowedHttpMethods;
  }

  public ServerConfig()
  {

  }

  @JsonProperty
  @Min(1)
  private int numThreads = getDefaultNumThreads();

  @JsonProperty
  @Min(1)
  private int queueSize = Integer.MAX_VALUE;

  @JsonProperty
  private boolean enableRequestLimit = false;

  @JsonProperty
  @NotNull
  private Period maxIdleTime = new Period("PT5m");

  @JsonProperty
  @Min(0)
  private long defaultQueryTimeout = TimeUnit.MINUTES.toMillis(5);

  @JsonProperty
  @Min(1)
  private long maxScatterGatherBytes = Long.MAX_VALUE;

  @JsonProperty
  @Min(1)
  private int maxSubqueryRows = 100000;

  @JsonProperty
  @Min(1)
  private long maxQueryTimeout = Long.MAX_VALUE;

  @JsonProperty
  private int maxRequestHeaderSize = 8 * 1024;

  @JsonProperty
  @NotNull
  private Period gracefulShutdownTimeout = Period.ZERO;

  @JsonProperty
  @NotNull
  private Period unannouncePropagationDelay = Period.ZERO;

  @JsonProperty
  @Min(0)
  private int inflateBufferSize = DEFAULT_GZIP_INFLATE_BUFFER_SIZE;

  @JsonProperty
  @Min(-1)
  @Max(9)
  private int compressionLevel = Deflater.DEFAULT_COMPRESSION;

  @JsonProperty
  private boolean enableForwardedRequestCustomizer = false;

  @JsonProperty
  @NotNull
  private List<String> allowedHttpMethods = ImmutableList.of();

  public int getNumThreads()
  {
    return numThreads;
  }

  public int getQueueSize()
  {
    return queueSize;
  }

  public boolean isEnableRequestLimit()
  {
    return enableRequestLimit;
  }

  public Period getMaxIdleTime()
  {
    return maxIdleTime;
  }

  public long getDefaultQueryTimeout()
  {
    return defaultQueryTimeout;
  }

  public long getMaxScatterGatherBytes()
  {
    return maxScatterGatherBytes;
  }

  public int getMaxSubqueryRows()
  {
    return maxSubqueryRows;
  }

  public long getMaxQueryTimeout()
  {
    return maxQueryTimeout;
  }

  public int getMaxRequestHeaderSize()
  {
    return maxRequestHeaderSize;
  }

  public Period getGracefulShutdownTimeout()
  {
    return gracefulShutdownTimeout;
  }

  public Period getUnannouncePropagationDelay()
  {
    return unannouncePropagationDelay;
  }

  public int getInflateBufferSize()
  {
    return inflateBufferSize;
  }

  public int getCompressionLevel()
  {
    return compressionLevel;
  }

  public boolean isEnableForwardedRequestCustomizer()
  {
    return enableForwardedRequestCustomizer;
  }

  @NotNull
  public List<String> getAllowedHttpMethods()
  {
    return allowedHttpMethods;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ServerConfig that = (ServerConfig) o;
    return numThreads == that.numThreads &&
           queueSize == that.queueSize &&
           enableRequestLimit == that.enableRequestLimit &&
           defaultQueryTimeout == that.defaultQueryTimeout &&
           maxScatterGatherBytes == that.maxScatterGatherBytes &&
           maxSubqueryRows == that.maxSubqueryRows &&
           maxQueryTimeout == that.maxQueryTimeout &&
           maxRequestHeaderSize == that.maxRequestHeaderSize &&
           inflateBufferSize == that.inflateBufferSize &&
           compressionLevel == that.compressionLevel &&
           enableForwardedRequestCustomizer == that.enableForwardedRequestCustomizer &&
           maxIdleTime.equals(that.maxIdleTime) &&
           gracefulShutdownTimeout.equals(that.gracefulShutdownTimeout) &&
           unannouncePropagationDelay.equals(that.unannouncePropagationDelay) &&
           allowedHttpMethods.equals(that.allowedHttpMethods);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        numThreads,
        queueSize,
        enableRequestLimit,
        maxIdleTime,
        defaultQueryTimeout,
        maxScatterGatherBytes,
        maxSubqueryRows,
        maxQueryTimeout,
        maxRequestHeaderSize,
        gracefulShutdownTimeout,
        unannouncePropagationDelay,
        inflateBufferSize,
        compressionLevel,
        enableForwardedRequestCustomizer,
        allowedHttpMethods
    );
  }

  @Override
  public String toString()
  {
    return "ServerConfig{" +
           "numThreads=" + numThreads +
           ", queueSize=" + queueSize +
           ", enableRequestLimit=" + enableRequestLimit +
           ", maxIdleTime=" + maxIdleTime +
           ", defaultQueryTimeout=" + defaultQueryTimeout +
           ", maxScatterGatherBytes=" + maxScatterGatherBytes +
           ", maxSubqueryRows=" + maxSubqueryRows +
           ", maxQueryTimeout=" + maxQueryTimeout +
           ", maxRequestHeaderSize=" + maxRequestHeaderSize +
           ", gracefulShutdownTimeout=" + gracefulShutdownTimeout +
           ", unannouncePropagationDelay=" + unannouncePropagationDelay +
           ", inflateBufferSize=" + inflateBufferSize +
           ", compressionLevel=" + compressionLevel +
           ", enableForwardedRequestCustomizer=" + enableForwardedRequestCustomizer +
           ", allowedMethods=" + allowedHttpMethods +
           '}';
  }

  public static int getDefaultNumThreads()
  {
    return Math.max(10, (JvmUtils.getRuntimeInfo().getAvailableProcessors() * 17) / 16 + 2) + 30;
  }
}
