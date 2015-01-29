/*
 * Druid - a distributed column store.
 * Copyright (C) 2015  Metamarkets Group Inc.
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

package io.druid.common.aws;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.common.base.Strings;

public class ConfigDrivenAwsCredentialsConfigProvider implements AWSCredentialsProvider
{
  private AWSCredentialsConfig config;

  public ConfigDrivenAwsCredentialsConfigProvider(AWSCredentialsConfig config) {
    this.config = config;
  }

  @Override
  public com.amazonaws.auth.AWSCredentials getCredentials()
  {
      if (!Strings.isNullOrEmpty(config.getAccessKey()) && !Strings.isNullOrEmpty(config.getSecretKey())) {
        return new com.amazonaws.auth.AWSCredentials() {
          @Override
          public String getAWSAccessKeyId() {
            return config.getAccessKey();
          }

          @Override
          public String getAWSSecretKey() {
            return config.getSecretKey();
          }
        };
      }
      throw new AmazonClientException("Unable to load AWS credentials from druid AWSCredentialsConfig");
  }

  @Override
  public void refresh() {}
}
