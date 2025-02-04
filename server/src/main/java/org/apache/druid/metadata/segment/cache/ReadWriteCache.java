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
import org.apache.druid.error.DruidException;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Cache with standard read/write locking.
 */
public abstract class ReadWriteCache implements DatasourceSegmentCache
{
  private final ReentrantReadWriteLock stateLock;
  private volatile boolean isStopped = false;

  public ReadWriteCache(boolean fair)
  {
    stateLock = new ReentrantReadWriteLock(fair);
  }

  /**
   * Stops this cache. Any subsequent read/write action performed on this cache
   * will throw a defensive DruidException.
   */
  public void stop()
  {
    withWriteLock(() -> {
      isStopped = true;
    });
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
      verifyCacheIsNotStopped();
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
      verifyCacheIsNotStopped();
      return action.get();
    }
    finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public <T> T read(DatasourceSegmentCache.Action<T> action) throws Exception
  {
    stateLock.readLock().lock();
    try {
      verifyCacheIsNotStopped();
      return action.perform();
    }
    finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public <T> T write(DatasourceSegmentCache.Action<T> action) throws Exception
  {
    stateLock.writeLock().lock();
    try {
      verifyCacheIsNotStopped();
      return action.perform();
    }
    finally {
      stateLock.writeLock().unlock();
    }
  }

  private void verifyCacheIsNotStopped()
  {
    if (isStopped) {
      throw DruidException.defensive("Cache is already stopped");
    }
  }

  @FunctionalInterface
  public interface Action
  {
    void perform();
  }
}
