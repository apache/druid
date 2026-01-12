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

package org.apache.druid.common.aws.v2;

import com.google.common.base.Strings;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;

public class ConfigDrivenAwsCredentialsConfigProvider implements AwsCredentialsProvider
{
  private final AWSCredentialsConfig config;

  public ConfigDrivenAwsCredentialsConfigProvider(AWSCredentialsConfig config)
  {
    this.config = config;
  }

  @Override
  public AwsCredentials resolveCredentials()
  {
    final String key = config.getAccessKey().getPassword();
    final String secret = config.getSecretKey().getPassword();
    if (!Strings.isNullOrEmpty(key) && !Strings.isNullOrEmpty(secret)) {
      return AwsBasicCredentials.create(key, secret);
    }
    throw SdkException.builder()
        .message("Unable to load AWS credentials from druid AWSCredentialsConfig")
        .build();
  }
}
