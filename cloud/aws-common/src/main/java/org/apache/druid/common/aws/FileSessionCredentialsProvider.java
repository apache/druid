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

import org.apache.druid.java.util.common.concurrent.Execs;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FileSessionCredentialsProvider implements AwsCredentialsProvider
{
  private final ScheduledExecutorService scheduler =
      Execs.scheduledSingleThreaded("FileSessionCredentialsProviderRefresh-%d");
  private final String sessionCredentialsFile;

  /**
   * This field doesn't need to be volatile. From the Java Memory Model point of view, volatile on this field changes
   * nothing and doesn't provide any extra guarantees.
   */
  private AwsSessionCredentials awsSessionCredentials;

  public FileSessionCredentialsProvider(String sessionCredentialsFile)
  {
    this.sessionCredentialsFile = sessionCredentialsFile;
    refresh();

    scheduler.scheduleAtFixedRate(this::refresh, 1, 1, TimeUnit.HOURS); // refresh every hour
  }

  @Override
  public AwsCredentials resolveCredentials()
  {
    return awsSessionCredentials;
  }

  private void refresh()
  {
    try {
      Properties props = new Properties();
      try (InputStream is = Files.newInputStream(Paths.get(sessionCredentialsFile))) {
        props.load(is);
      }

      String sessionToken = props.getProperty("sessionToken");
      String accessKey = props.getProperty("accessKey");
      String secretKey = props.getProperty("secretKey");

      awsSessionCredentials = AwsSessionCredentials.create(accessKey, secretKey, sessionToken);
    }
    catch (IOException e) {
      throw new RuntimeException("cannot refresh AWS credentials", e);
    }
  }
}
