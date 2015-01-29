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

import com.amazonaws.auth.AWSCredentialsProvider;

public class LazyFileSessionCredentialsProvider implements AWSCredentialsProvider
{
  private AWSCredentialsConfig config;
  private FileSessionCredentialsProvider provider;

  public LazyFileSessionCredentialsProvider(AWSCredentialsConfig config) {
    this.config = config;
  }

  private FileSessionCredentialsProvider getUnderlyingProvider() {
    if (provider == null) {
      synchronized (config) {
        if (provider == null) {
          provider = new FileSessionCredentialsProvider(config.getFileSessionCredentials());
        }
      }
    }
    return provider;
  }

  @Override
  public com.amazonaws.auth.AWSCredentials getCredentials()
  {
    return getUnderlyingProvider().getCredentials();
  }

  @Override
  public void refresh() {
    getUnderlyingProvider().refresh();
  }
}
