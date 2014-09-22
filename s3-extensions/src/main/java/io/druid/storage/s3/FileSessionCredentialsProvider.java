/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.storage.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FileSessionCredentialsProvider implements AWSCredentialsProvider {
  private final String sessionCredentials;
  private volatile String sessionToken;
  private volatile String accessKey;
  private volatile String secretKey;

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("FileSessionCredentialsProviderRefresh-%d")
          .setDaemon(true).build()
  );

  public FileSessionCredentialsProvider(String sessionCredentials) {
    this.sessionCredentials = sessionCredentials;
    refresh();

    scheduler.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        refresh();
      }
    }, 1, 1, TimeUnit.HOURS); // refresh every hour
  }

  @Override
  public AWSCredentials getCredentials() {
    return new AWSSessionCredentials() {
      @Override
      public String getSessionToken() {
        return sessionToken;
      }

      @Override
      public String getAWSAccessKeyId() {
        return accessKey;
      }

      @Override
      public String getAWSSecretKey() {
        return secretKey;
      }
    };
  }

  @Override
  public void refresh() {
    try {
      Properties props = new Properties();
      InputStream is = new FileInputStream(new File(sessionCredentials));
      props.load(is);
      is.close();

      sessionToken = props.getProperty("sessionToken");
      accessKey = props.getProperty("accessKey");
      secretKey = props.getProperty("secretKey");
    } catch (IOException e) {
      throw new RuntimeException("cannot refresh AWS credentials", e);
    }
  }
}
