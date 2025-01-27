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

import com.google.common.base.Supplier;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class BaseCache implements SegmentsMetadataCache.DataSource
{
  private final ReentrantReadWriteLock stateLock;

  public BaseCache(boolean fair)
  {
    stateLock = new ReentrantReadWriteLock(fair);
  }

  public void withWriteLock(Action action)
  {
    withWriteLock(() -> {
      action.perform();
      return 0;
    });
  }

  public <T> T withWriteLock(Supplier<T> action)
  {
    stateLock.writeLock().lock();
    try {
      return action.get();
    }
    finally {
      stateLock.writeLock().unlock();
    }
  }

  public <T> T withReadLock(Supplier<T> action)
  {
    stateLock.readLock().lock();
    try {
      return action.get();
    }
    finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public <T> T withReadLock(SegmentsMetadataCache.Action<T> action) throws Exception
  {
    stateLock.readLock().lock();
    try {
      return action.perform(this);
    }
    finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public <T> T withWriteLock(SegmentsMetadataCache.Action<T> action) throws Exception
  {
    stateLock.writeLock().lock();
    try {
      return action.perform(this);
    }
    finally {
      stateLock.writeLock().unlock();
    }
  }

  @FunctionalInterface
  public interface Action
  {
    void perform();
  }
}
