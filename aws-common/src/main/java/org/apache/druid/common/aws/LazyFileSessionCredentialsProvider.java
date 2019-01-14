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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

public class LazyFileSessionCredentialsProvider implements AWSCredentialsProvider
{
  private final AWSCredentialsConfig config;

  /**
   * The field is declared volatile in order to ensure safe publication of the object
   * in {@link #getUnderlyingProvider()} without worrying about final modifiers
   * on the fields of the created object
   *
   * @see <a href="https://github.com/apache/incubator-druid/pull/6662#discussion_r237013157">
   *     https://github.com/apache/incubator-druid/pull/6662#discussion_r237013157</a>
   */
  @MonotonicNonNull
  private volatile FileSessionCredentialsProvider provider;

  public LazyFileSessionCredentialsProvider(AWSCredentialsConfig config)
  {
    this.config = config;
  }

  @EnsuresNonNull("provider")
  private FileSessionCredentialsProvider getUnderlyingProvider()
  {
    FileSessionCredentialsProvider syncedProvider = provider;
    if (syncedProvider == null) {
      synchronized (config) {
        syncedProvider = provider;
        if (syncedProvider == null) {
          syncedProvider = new FileSessionCredentialsProvider(config.getFileSessionCredentials());
          provider = syncedProvider;
        }
      }
    }
    return syncedProvider;
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
