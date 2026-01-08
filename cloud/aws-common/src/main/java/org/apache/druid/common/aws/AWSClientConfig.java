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

import com.fasterxml.jackson.annotation.JsonProperty;

public class AWSClientConfig
{
  // Default values matching AWS SDK v2 defaults
  private static final boolean DEFAULT_CHUNKED_ENCODING_DISABLED = false;
  private static final boolean DEFAULT_PATH_STYLE_ACCESS = false;
  private static final boolean DEFAULT_FORCE_GLOBAL_BUCKET_ACCESS_ENABLED = false;
  private static final int DEFAULT_CONNECTION_TIMEOUT = 10_000; // 10 seconds
  private static final int DEFAULT_SOCKET_TIMEOUT = 50_000; // 50 seconds
  private static final int DEFAULT_MAX_CONNECTIONS = 50;

  @JsonProperty
  private String protocol = "https"; // The default of aws-java-sdk

  @JsonProperty
  private boolean disableChunkedEncoding = DEFAULT_CHUNKED_ENCODING_DISABLED;

  @JsonProperty
  private boolean enablePathStyleAccess = DEFAULT_PATH_STYLE_ACCESS;

  @JsonProperty
  protected boolean forceGlobalBucketAccessEnabled = DEFAULT_FORCE_GLOBAL_BUCKET_ACCESS_ENABLED;

  @JsonProperty
  private int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

  @JsonProperty
  private int socketTimeout = DEFAULT_SOCKET_TIMEOUT;

  @JsonProperty
  private int maxConnections = DEFAULT_MAX_CONNECTIONS;

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

  public boolean isForceGlobalBucketAccessEnabled()
  {
    return forceGlobalBucketAccessEnabled;
  }

  public int getConnectionTimeout()
  {
    return connectionTimeout;
  }

  public int getSocketTimeout()
  {
    return socketTimeout;
  }

  public int getMaxConnections()
  {
    return maxConnections;
  }

  @Override
  public String toString()
  {
    return "AWSClientConfig{" +
           "protocol='" + protocol + '\'' +
           ", disableChunkedEncoding=" + disableChunkedEncoding +
           ", enablePathStyleAccess=" + enablePathStyleAccess +
           ", forceGlobalBucketAccessEnabled=" + forceGlobalBucketAccessEnabled +
           ", connectionTimeout=" + connectionTimeout +
           ", socketTimeout=" + socketTimeout +
           ", maxConnections=" + maxConnections +
           '}';
  }
}
