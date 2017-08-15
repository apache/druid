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

public class LazyFileSessionCredentialsProvider implements AWSCredentialsProvider
{
  private AWSCredentialsConfig config;
  private FileSessionCredentialsProvider provider;

  public LazyFileSessionCredentialsProvider(AWSCredentialsConfig config)
  {
    this.config = config;
  }

  private FileSessionCredentialsProvider getUnderlyingProvider()
  {
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
  public AWSCredentials getCredentials()
  {
    return getUnderlyingProvider().getCredentials();
  }

  @Override
  public void refresh()
  {
    getUnderlyingProvider().refresh();
  }
}
