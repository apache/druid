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
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.common.base.Strings;

public class ConfigDrivenAwsCredentialsConfigProvider implements AWSCredentialsProvider
{
  private AWSCredentialsConfig config;

  public ConfigDrivenAwsCredentialsConfigProvider(AWSCredentialsConfig config)
  {
    this.config = config;
  }

  @Override
  public AWSCredentials getCredentials()
  {
    final String key = config.getAccessKey().getPassword();
    final String secret = config.getSecretKey().getPassword();
    if (!Strings.isNullOrEmpty(key) && !Strings.isNullOrEmpty(secret)) {
      return new AWSCredentials()
      {
        @Override
        public String getAWSAccessKeyId()
        {
          return key;
        }

        @Override
        public String getAWSSecretKey()
        {
          return secret;
        }
      };
    }
    throw new AmazonClientException("Unable to load AWS credentials from druid AWSCredentialsConfig");
  }

  @Override
  public void refresh() {}
}
