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

package org.apache.druid.metadata.segment.cache;

import org.apache.druid.client.DataSourcesSnapshot;

public class NoopSegmentMetadataCache implements SegmentMetadataCache
{
  private static final NoopSegmentMetadataCache INSTANCE = new NoopSegmentMetadataCache();

  public static NoopSegmentMetadataCache instance()
  {
    return INSTANCE;
  }

  private NoopSegmentMetadataCache()
  {
    // no instantiation
  }

  @Override
  public void start()
  {

  }

  @Override
  public void stop()
  {

  }

  @Override
  public void becomeLeader()
  {

  }

  @Override
  public void stopBeingLeader()
  {

  }

  @Override
  public boolean isEnabled()
  {
    return false;
  }

  @Override
  public boolean isSyncedForRead()
  {
    return false;
  }

  @Override
  public DataSourcesSnapshot getDatasourcesSnapshot()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T readCacheForDataSource(String dataSource, Action<T> readAction)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T writeCacheForDataSource(String dataSource, Action<T> writeAction)
  {
    throw new UnsupportedOperationException();
  }
}
