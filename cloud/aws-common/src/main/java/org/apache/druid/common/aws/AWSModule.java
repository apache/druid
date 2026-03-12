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

import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.ec2.Ec2Client;

public class AWSModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.s3", AWSCredentialsConfig.class);
    JsonConfigProvider.bind(binder, "druid.s3", AWSClientConfig.class);
    JsonConfigProvider.bind(binder, "druid.s3.proxy", AWSProxyConfig.class);
    JsonConfigProvider.bind(binder, "druid.s3.endpoint", AWSEndpointConfig.class);
  }

  @Provides
  @LazySingleton
  public AwsCredentialsProvider getAWSCredentialsProvider(final AWSCredentialsConfig config)
  {
    return AWSCredentialsUtils.defaultAWSCredentialsProviderChain(config);
  }

  @Provides
  @LazySingleton
  public Ec2Client getEc2Client(AwsCredentialsProvider credentials)
  {
    return Ec2Client.builder()
        .credentialsProvider(credentials)
        .build();
  }
}
