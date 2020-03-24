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

package org.apache.druid.testing.utils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.util.AwsHostNameUtils;

import java.io.FileInputStream;
import java.util.Properties;

public class KinesisAdminClient
{
  private AmazonKinesisClient amazonKinesisClient;

  KinesisAdminClient() throws Exception
  {
    String pathToConfigFile = System.getProperty("override.config.path");
    Properties prop = new Properties();
    prop.load(new FileInputStream(pathToConfigFile));

    AWSStaticCredentialsProvider credentials = new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(
            prop.getProperty("druid_kinesis_accessKey"),
            prop.getProperty("druid_kinesis_secretKey")
        )
    );
    amazonKinesisClient = AmazonKinesisClientBuilder.standard()
                              .withCredentials(credentials)
                              .withClientConfiguration(new ClientConfiguration())
                              .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                                  endpoint,
                                  AwsHostNameUtils.parseRegion(
                                      endpoint,
                                      null
                                  )
                              )).build();
  }
}
