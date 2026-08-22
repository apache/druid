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

package org.apache.druid.common.aws;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import jakarta.validation.constraints.Min;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.utils.RuntimeInfo;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;

import javax.annotation.Nullable;
import java.util.Arrays;

public class AWSClientConfig
{
  // Default values matching AWS SDK v2 defaults
  private static final boolean DEFAULT_CHUNKED_ENCODING_DISABLED = false;
  private static final boolean DEFAULT_PATH_STYLE_ACCESS = false;

  private static final int DEFAULT_CONNECTION_TIMEOUT_MILLIS = 10_000;
  private static final int DEFAULT_SOCKET_TIMEOUT_MILLIS = 50_000;
  /** AWS SDK v2's own default. */
  private static final int DEFAULT_MAX_CONNECTIONS_FLOOR = 50;

  /**
   * Retry strategy family. Declared as an enum so an unrecognised value is rejected while the config is bound at
   * startup, rather than when a client is first built.
   */
  public enum RetryMode
  {
    STANDARD {
      @Override
      RetryStrategy createStrategy()
      {
        // Pass true to ensure we get the new standard AWS SDKv2 retry behavior and not legacy behavior.
        return AwsRetryStrategy.standardRetryStrategy(true);
      }
    },
    ADAPTIVE {
      @Override
      RetryStrategy createStrategy()
      {
        // Standard plus a client-side rate limiter, which unlike standard can delay or block the initial request,
        // not just retries. The limiter belongs to one client instance and covers every request that client makes,
        // so throttling on one key prefix also slows requests to prefixes that are not being throttled.
        return AwsRetryStrategy.adaptiveRetryStrategy(true);
      }
    },
    LEGACY {
      @Override
      RetryStrategy createStrategy()
      {
        // Deliberately left on the pre-standard behavior: this mode exists so a deployment can get back to what it
        // had before, which is the opposite of what the opt-in above asks for.
        return AwsRetryStrategy.legacyRetryStrategy();
      }
    };

    abstract RetryStrategy createStrategy();

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(name());
    }

    @JsonCreator
    public static RetryMode fromString(String value)
    {
      for (RetryMode mode : values()) {
        if (mode.name().equalsIgnoreCase(value)) {
          return mode;
        }
      }
      throw new IAE("Invalid druid.s3.retryMode[%s]. Must be one of %s.", value, Arrays.toString(values()));
    }
  }

  /**
   * Used by {@link #getMaxConnections} to scale the default connection pool with host size so hosts large enough to
   * do a lot of concurrent deep-storage I/O (e.g. virtual-storage historicals fanning out on-demand loads to S3)
   * aren't bottlenecked at the SDK's connection pool. The field initializer covers direct construction (no Jackson);
   * Jackson overwrites with the injected {@link RuntimeInfo} during deserialization.
   */
  @JacksonInject
  private final RuntimeInfo runtimeInfo = new RuntimeInfo();

  @JsonProperty
  private String protocol = "https"; // The default of aws-java-sdk

  @JsonProperty
  private boolean disableChunkedEncoding = DEFAULT_CHUNKED_ENCODING_DISABLED;

  @JsonProperty
  private boolean enablePathStyleAccess = DEFAULT_PATH_STYLE_ACCESS;

  /**
   * @deprecated Use {@link #crossRegionAccessEnabled} instead.
   */
  @Deprecated
  @JsonProperty
  @Nullable
  protected Boolean forceGlobalBucketAccessEnabled;

  @JsonProperty
  @Nullable
  private Boolean crossRegionAccessEnabled;

  @JsonProperty
  private int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT_MILLIS;

  @JsonProperty
  private int socketTimeout = DEFAULT_SOCKET_TIMEOUT_MILLIS;

  /**
   * Null means use the dynamic default in {@link #getMaxConnections} ({@code max(50, 4 × availableProcessors)});
   * any explicit value set in JSON wins.
   */
  @JsonProperty
  @Nullable
  private Integer maxConnections = null;

  /**
   * Retry strategy applied to every AWS client built from this config.
   */
  @JsonProperty
  private RetryMode retryMode = RetryMode.STANDARD;

  /**
   * Total attempts per request, including the first. A value of 1 disables
   * retries. Null leaves the count that {@link #retryMode} defines for itself, which AWS tunes alongside that mode's
   * backoff and retry quota.
   * <p>
   * This counts HTTP requests. Druid layers its own retries on top (see {@code S3Utils#retryS3Operation}) and the two
   * multiply, but they are not equivalent: an attempt here re-sends a single request, whereas a Druid-level retry
   * repeats a whole operation, such as re-uploading an entire segment.
   */
  @JsonProperty
  @Nullable
  @Min(1)
  private Integer maxAttempts = null;

  public String getProtocol()
  {
    return protocol;
  }

  public boolean isDisableChunkedEncoding()
  {
    return disableChunkedEncoding;
  }

  public boolean isEnablePathStyleAccess()
  {
    return enablePathStyleAccess;
  }

  /**
   * @deprecated Use {@link #isCrossRegionAccessEnabled()} instead.
   */
  @Deprecated
  @Nullable
  public Boolean isForceGlobalBucketAccessEnabled()
  {
    return forceGlobalBucketAccessEnabled;
  }

  @Nullable
  public Boolean getCrossRegionAccessEnabled()
  {
    return crossRegionAccessEnabled;
  }

  /**
   * Resolves cross-region access setting. Precedence:
   * 1. If crossRegionAccessEnabled is explicitly set, use it.
   * 2. If forceGlobalBucketAccessEnabled (deprecated) is explicitly set, use it.
   * 3. Otherwise, default to false.
   */
  public boolean isCrossRegionAccessEnabled()
  {
    if (crossRegionAccessEnabled != null) {
      return crossRegionAccessEnabled;
    }
    if (forceGlobalBucketAccessEnabled != null) {
      return forceGlobalBucketAccessEnabled;
    }
    return false;
  }

  public int getConnectionTimeoutMillis()
  {
    return connectionTimeout;
  }

  public int getSocketTimeoutMillis()
  {
    return socketTimeout;
  }

  public int getMaxConnections()
  {
    if (maxConnections != null) {
      return maxConnections;
    }
    return Math.max(DEFAULT_MAX_CONNECTIONS_FLOOR, 4 * runtimeInfo.getAvailableProcessors());
  }

  public RetryMode getRetryMode()
  {
    return retryMode;
  }

  @Nullable
  public Integer getMaxAttempts()
  {
    return maxAttempts;
  }

  /**
   * Builds the strategy to hand to {@code ClientOverrideConfiguration.retryStrategy}. Kept as a plain function of the
   * config because a built AWS client does not expose the strategy it was given, so this is the only place the
   * mapping can be tested.
   * <p>
   * Returns a new instance per call; clients must not share one, since the strategies hold their circuit-breaker
   * quota (and, for adaptive, their rate limiter) on the instance.
   */
  public RetryStrategy getRetryStrategy()
  {
    return withMaxAttempts(retryMode.createStrategy());
  }

  /**
   * Overrides the attempt count only when one is configured, so an unset {@link #maxAttempts} leaves whatever
   * the chosen mode defines for itself.
   */
  private RetryStrategy withMaxAttempts(RetryStrategy strategy)
  {
    if (maxAttempts == null) {
      return strategy;
    }
    return strategy.toBuilder().maxAttempts(maxAttempts).build();
  }

  @Override
  public String toString()
  {
    return "AWSClientConfig{" +
           "protocol='" + protocol + '\'' +
           ", disableChunkedEncoding=" + disableChunkedEncoding +
           ", enablePathStyleAccess=" + enablePathStyleAccess +
           ", crossRegionAccessEnabled=" + isCrossRegionAccessEnabled() +
           ", connectionTimeout=" + connectionTimeout +
           ", socketTimeout=" + socketTimeout +
           ", maxConnections=" + getMaxConnections() +
           ", retryMode='" + retryMode + '\'' +
           ", maxRetryAttempts=" + maxAttempts +
           '}';
  }
}
