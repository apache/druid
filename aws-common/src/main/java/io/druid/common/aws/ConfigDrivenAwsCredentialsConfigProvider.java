/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.common.aws;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.common.base.Strings;

public class ConfigDrivenAwsCredentialsConfigProvider implements AWSCredentialsProvider
{
  private AWSCredentialsConfig config;

  public ConfigDrivenAwsCredentialsConfigProvider(AWSCredentialsConfig config) {
    this.config = config;
  }

  @Override
  public com.amazonaws.auth.AWSCredentials getCredentials()
  {
      if (!Strings.isNullOrEmpty(config.getAccessKey()) && !Strings.isNullOrEmpty(config.getSecretKey())) {
        return new com.amazonaws.auth.AWSCredentials() {
          @Override
          public String getAWSAccessKeyId() {
            return config.getAccessKey();
          }

          @Override
          public String getAWSSecretKey() {
            return config.getSecretKey();
          }
        };
      }
      throw new AmazonClientException("Unable to load AWS credentials from druid AWSCredentialsConfig");
  }

  @Override
  public void refresh() {}
}
