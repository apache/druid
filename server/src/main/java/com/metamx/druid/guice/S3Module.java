/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.guice;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.ProvisionException;
import com.metamx.druid.loading.AWSCredentialsConfig;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;

/**
 */
public class S3Module implements Module
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
  public org.jets3t.service.security.AWSCredentials  getJets3tAWSCredentials(AWSCredentialsConfig config)
  {
    return new org.jets3t.service.security.AWSCredentials(config.getAccessKey(), config.getSecretKey());
  }

  @Provides
  @LazySingleton
  public RestS3Service getRestS3Service(org.jets3t.service.security.AWSCredentials credentials)
  {
    try {
      return new RestS3Service(credentials);
    }
    catch (S3ServiceException e) {
      throw new ProvisionException("Unable to create a RestS3Service", e);
    }
  }

  @Provides
  @LazySingleton
  public AmazonEC2 getEc2Client(AWSCredentials credentials)
  {
    return new AmazonEC2Client(credentials);
  }
}
