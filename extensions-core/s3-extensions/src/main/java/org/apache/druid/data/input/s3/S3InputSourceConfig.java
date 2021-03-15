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

/**
 * Contains properties for s3 input source.
 * Properties can be specified by ingestionSpec which will override system default.
 */
public class S3InputSourceConfig
{
  @Nullable
  @JsonProperty
  private String awsAssumedRoleArn;
  @Nullable
  @JsonProperty
  private String externalId;
  @JsonProperty
  private PasswordProvider accessKeyId;
  @JsonProperty
  private PasswordProvider secretAccessKey;

  @JsonCreator
  public S3InputSourceConfig(
      @JsonProperty("accessKeyId") @Nullable PasswordProvider accessKeyId,
      @JsonProperty("secretAccessKey") @Nullable PasswordProvider secretAccessKey,
      @JsonProperty("awsAssumedRoleArn") @Nullable String awsAssumedRoleArn,
      @JsonProperty("awsExternalId") @Nullable String exteranlId
  )
  {
    this.awsAssumedRoleArn = awsAssumedRoleArn;
    this.externalId = exteranlId;
    if (accessKeyId != null || secretAccessKey != null) {
      this.accessKeyId = Preconditions.checkNotNull(accessKeyId, "accessKeyId cannot be null if secretAccessKey is given");
      this.secretAccessKey = Preconditions.checkNotNull(secretAccessKey, "secretAccessKey cannot be null if accessKeyId is given");
    }
  }

  @Nullable
  public String getAwsAssumedRoleArn()
  {
    return awsAssumedRoleArn;
  }

  @Nullable
  public String getAwsAssumeRoleExternalId()
  {
    return externalId;
  }

  public PasswordProvider getAccessKeyId()
  {
    return accessKeyId;
  }

  public PasswordProvider getSecretAccessKey()
  {
    return secretAccessKey;
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
           "awsAssumedRoleArn='" + awsAssumedRoleArn + '\'' +
           ", externalId='" + externalId + '\'' +
           ", accessKeyId=" + accessKeyId +
           ", secretAccessKey=" + secretAccessKey +
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

    if (awsAssumedRoleArn != null
        ? !awsAssumedRoleArn.equals(that.awsAssumedRoleArn)
        : that.awsAssumedRoleArn != null) {
      return false;
    }
    if (externalId != null ? !externalId.equals(that.externalId) : that.externalId != null) {
      return false;
    }
    if (accessKeyId != null ? !accessKeyId.equals(that.accessKeyId) : that.accessKeyId != null) {
      return false;
    }
    return secretAccessKey != null ? secretAccessKey.equals(that.secretAccessKey) : that.secretAccessKey == null;
  }

  @Override
  public int hashCode()
  {
    int result = awsAssumedRoleArn != null ? awsAssumedRoleArn.hashCode() : 0;
    result = 31 * result + (externalId != null ? externalId.hashCode() : 0);
    result = 31 * result + (accessKeyId != null ? accessKeyId.hashCode() : 0);
    result = 31 * result + (secretAccessKey != null ? secretAccessKey.hashCode() : 0);
    return result;
  }
}
