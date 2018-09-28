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

package org.apache.druid.indexing.kinesis.aws;

import org.apache.druid.common.aws.AWSCredentialsConfig;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.metadata.PasswordProvider;

public class ConstructibleAWSCredentialsConfig extends AWSCredentialsConfig
{
  private final String accessKey;
  private final String secretKey;
  private final String fileSessionCredentials;

  public ConstructibleAWSCredentialsConfig(String accessKey, String secretKey)
  {
    this(accessKey, secretKey, null);
  }

  public ConstructibleAWSCredentialsConfig(String accessKey, String secretKey, String fileSessionCredentials)
  {
    this.accessKey = accessKey != null ? accessKey : "";
    this.secretKey = secretKey != null ? secretKey : "";
    this.fileSessionCredentials = fileSessionCredentials != null ? fileSessionCredentials : "";
  }

  @Override
  public PasswordProvider getAccessKey()
  {
    return DefaultPasswordProvider.fromString(accessKey);
  }

  @Override
  public PasswordProvider getSecretKey()
  {
    return DefaultPasswordProvider.fromString(secretKey);
  }

  @Override
  public String getFileSessionCredentials()
  {
    return fileSessionCredentials;
  }
}
