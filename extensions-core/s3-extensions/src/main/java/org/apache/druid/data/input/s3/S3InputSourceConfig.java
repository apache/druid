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

package org.apache.druid.data.input.s3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.metadata.PasswordProvider;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Contains properties for s3 input source.
 * Properties can be specified by ingestionSpec which will override system default.
 */
public class S3InputSourceConfig
{
  @JsonProperty
  private String assumeRoleArn;
  @JsonProperty
  private String assumeRoleExternalId;
  @JsonProperty
  private PasswordProvider accessKeyId;
  @JsonProperty
  private PasswordProvider secretAccessKey;
  @JsonProperty
  private String region;
  @JsonProperty
  private String endpointUrl;
  @JsonProperty
  private String endpointSigningRegion;

  @JsonCreator
  public S3InputSourceConfig(
      @JsonProperty("accessKeyId") @Nullable PasswordProvider accessKeyId,
      @JsonProperty("secretAccessKey") @Nullable PasswordProvider secretAccessKey,
      @JsonProperty("assumeRoleArn") @Nullable String assumeRoleArn,
      @JsonProperty("assumeRoleExternalId") @Nullable String assumeRoleExternalId,
      @JsonProperty("region") @Nullable String region,
      @JsonProperty("endpointUrl") @Nullable String endpointUrl,
      @JsonProperty("endpointSigningRegion") @Nullable String endpointSigningRegion
  )
  {
    this.assumeRoleArn = assumeRoleArn;
    this.assumeRoleExternalId = assumeRoleExternalId;
    if (accessKeyId != null || secretAccessKey != null) {
      this.accessKeyId = Preconditions.checkNotNull(accessKeyId, "accessKeyId cannot be null if secretAccessKey is given");
      this.secretAccessKey = Preconditions.checkNotNull(secretAccessKey, "secretAccessKey cannot be null if accessKeyId is given");
    }
    this.region = region;
    this.endpointUrl = endpointUrl;
    this.endpointSigningRegion = endpointSigningRegion;
  }

  @Nullable
  public String getAssumeRoleArn()
  {
    return assumeRoleArn;
  }

  @Nullable
  public String getAssumeRoleExternalId()
  {
    return assumeRoleExternalId;
  }

  @Nullable
  public PasswordProvider getAccessKeyId()
  {
    return accessKeyId;
  }

  @Nullable
  public PasswordProvider getSecretAccessKey()
  {
    return secretAccessKey;
  }

  @Nullable
  public String getRegion()
  {
    return region;
  }

  @Nullable
  public String getEndpointUrl()
  {
    return endpointUrl;
  }

  @Nullable
  public String getEndpointSigningRegion()
  {
    return endpointSigningRegion;
  }

  @JsonIgnore
  public boolean isCredentialsConfigured()
  {
    return accessKeyId != null &&
           secretAccessKey != null;
  }

  @Override
  public String toString()
  {
    return "S3InputSourceConfig{" +
           "assumeRoleArn='" + assumeRoleArn + '\'' +
           ", assumeRoleExternalId='" + assumeRoleExternalId + '\'' +
           ", accessKeyId=" + accessKeyId +
           ", secretAccessKey=" + secretAccessKey +
           ", region='" + region + '\'' +
           ", endpointUrl='" + endpointUrl + '\'' +
           ", endpointSigningRegion='" + endpointSigningRegion + '\'' +
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
    S3InputSourceConfig that = (S3InputSourceConfig) o;
    return Objects.equals(assumeRoleArn, that.assumeRoleArn) &&
           Objects.equals(assumeRoleExternalId, that.assumeRoleExternalId) &&
           Objects.equals(accessKeyId, that.accessKeyId) &&
           Objects.equals(secretAccessKey, that.secretAccessKey) &&
           Objects.equals(region, that.region) &&
           Objects.equals(endpointUrl, that.endpointUrl) &&
           Objects.equals(endpointSigningRegion, that.endpointSigningRegion);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        assumeRoleArn,
        assumeRoleExternalId,
        accessKeyId,
        secretAccessKey,
        region,
        endpointUrl,
        endpointSigningRegion
    );
  }
}
