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

import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;

public class AWSCredentialsUtils
{
  public static AwsCredentialsProviderChain defaultAWSCredentialsProviderChain(final AWSCredentialsConfig config)
  {
    return AwsCredentialsProviderChain.of(
        new ConfigDrivenAwsCredentialsConfigProvider(config),
        new LazyFileSessionCredentialsProvider(config),
        EnvironmentVariableCredentialsProvider.create(),
        SystemPropertyCredentialsProvider.create(),
        WebIdentityTokenFileCredentialsProvider.create(),
        ProfileCredentialsProvider.create(),
        ContainerCredentialsProvider.builder().build(),
        InstanceProfileCredentialsProvider.create()
    );
  }
}
