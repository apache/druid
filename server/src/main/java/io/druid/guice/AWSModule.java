/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.guice;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;

/**
 */
public class AWSModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.s3", AWSCredentialsConfig.class);
  }

  @Provides
  @LazySingleton
  public AWSCredentials getAWSCredentials(AWSCredentialsConfig config)
  {
    return new BasicAWSCredentials(config.getAccessKey(), config.getSecretKey());
  }

  @Provides
  @LazySingleton
  public AmazonEC2 getEc2Client(AWSCredentials credentials)
  {
    return new AmazonEC2Client(credentials);
  }

  public static class AWSCredentialsConfig
  {
    @JsonProperty
    private String accessKey = "";

    @JsonProperty
    private String secretKey = "";

    public String getAccessKey()
    {
      return accessKey;
    }

    public String getSecretKey()
    {
      return secretKey;
    }
  }

}
