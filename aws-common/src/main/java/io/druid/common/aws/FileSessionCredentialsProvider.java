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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import io.druid.concurrent.Execs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FileSessionCredentialsProvider implements AWSCredentialsProvider
{
  private final String sessionCredentials;
  private volatile String sessionToken;
  private volatile String accessKey;
  private volatile String secretKey;

  private final ScheduledExecutorService scheduler =
      Execs.scheduledSingleThreaded("FileSessionCredentialsProviderRefresh-%d");

  public FileSessionCredentialsProvider(String sessionCredentials)
  {
    this.sessionCredentials = sessionCredentials;
    refresh();

    scheduler.scheduleAtFixedRate(this::refresh, 1, 1, TimeUnit.HOURS); // refresh every hour
  }

  @Override
  public AWSCredentials getCredentials()
  {
    return new AWSSessionCredentials()
    {
      @Override
      public String getSessionToken()
      {
        return sessionToken;
      }

      @Override
      public String getAWSAccessKeyId()
      {
        return accessKey;
      }

      @Override
      public String getAWSSecretKey()
      {
        return secretKey;
      }
    };
  }

  @Override
  public void refresh()
  {
    try {
      Properties props = new Properties();
      InputStream is = new FileInputStream(new File(sessionCredentials));
      props.load(is);
      is.close();

      sessionToken = props.getProperty("sessionToken");
      accessKey = props.getProperty("accessKey");
      secretKey = props.getProperty("secretKey");
    }
    catch (IOException e) {
      throw new RuntimeException("cannot refresh AWS credentials", e);
    }
  }
}
