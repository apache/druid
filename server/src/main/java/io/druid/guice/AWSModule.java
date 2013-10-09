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
