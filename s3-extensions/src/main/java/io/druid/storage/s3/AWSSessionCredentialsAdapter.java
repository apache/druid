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

import com.amazonaws.auth.AWSCredentialsProvider;
import org.jets3t.service.security.AWSSessionCredentials;

public class AWSSessionCredentialsAdapter extends AWSSessionCredentials {
  private final AWSCredentialsProvider provider;

  public AWSSessionCredentialsAdapter(AWSCredentialsProvider provider) {
    super(null, null, null);
    if(provider.getCredentials() instanceof com.amazonaws.auth.AWSSessionCredentials)
      this.provider = provider;
    else
      throw new IllegalArgumentException("provider does not contain session credentials");
  }

  @Override
  protected String getTypeName() {
    return "AWSSessionCredentialsAdapter";
  }

  @Override
  public String getVersionPrefix() {
    return "AWSSessionCredentialsAdapter, version: ";
  }

  @Override
  public String getAccessKey() {
    return provider.getCredentials().getAWSAccessKeyId();
  }

  @Override
  public String getSecretKey() {
    return provider.getCredentials().getAWSSecretKey();
  }

  public String getSessionToken() {
    com.amazonaws.auth.AWSSessionCredentials sessionCredentials =
        (com.amazonaws.auth.AWSSessionCredentials) provider.getCredentials();
    return sessionCredentials.getSessionToken();
  }
}
