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

package org.apache.druid.data.input.aliyun;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.metadata.PasswordProvider;

import java.util.Objects;

/**
 * Contains properties for aliyun OSS input source.
 * Properties can be specified by ingestionSpec which will override system default.
 */
public class OssClientConfig
{
  @JsonCreator
  public OssClientConfig(
      @JsonProperty("endpoint") String endpoint,
      @JsonProperty("accessKey") PasswordProvider accessKey,
      @JsonProperty("secretKey") PasswordProvider secretKey
  )
  {
    this.accessKey = Preconditions.checkNotNull(
        accessKey,
        "accessKey cannot be null"
    );
    this.secretKey = Preconditions.checkNotNull(
        secretKey,
        "secretKey cannot be null"
    );
    this.endpoint = endpoint;
  }

  @JsonProperty
  private String endpoint;

  @JsonProperty
  private PasswordProvider accessKey;

  @JsonProperty
  private PasswordProvider secretKey;

  public String getEndpoint()
  {
    return endpoint;
  }

  public PasswordProvider getAccessKey()
  {
    return accessKey;
  }

  public PasswordProvider getSecretKey()
  {
    return secretKey;
  }

  @JsonIgnore
  public boolean isCredentialsConfigured()
  {
    return accessKey != null &&
           secretKey != null;
  }

  @Override
  public String toString()
  {
    return "OssInputSourceConfig{" +
           "endpoint=" + endpoint +
           "accessKeyId=" + accessKey +
           ", secretAccessKey=" + secretKey +
           '}';
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
    OssClientConfig that = (OssClientConfig) o;
    return Objects.equals(accessKey, that.accessKey) &&
           Objects.equals(secretKey, that.secretKey) &&
           Objects.equals(endpoint, that.endpoint);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(accessKey, secretKey, endpoint);
  }

  public OSS buildClient()
  {
    return new OSSClientBuilder().build(endpoint, accessKey.getPassword(), secretKey.getPassword());
  }
}
