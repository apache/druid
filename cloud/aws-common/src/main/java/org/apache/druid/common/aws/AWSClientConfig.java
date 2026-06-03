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
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.utils.RuntimeInfo;

import javax.annotation.Nullable;

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
           '}';
  }
}
