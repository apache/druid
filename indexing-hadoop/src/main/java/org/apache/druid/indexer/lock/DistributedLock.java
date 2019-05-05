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

package org.apache.druid.indexer.lock;

/**
 * Modified from the DistributedLock in https://github.com/apache/kylin
 *
 * A distributed lock. Every instance is owned by a client, on whose behalf locks are acquired and/or released.
 *
 * Implementation must ensure all <code>lockPath</code> will be prefix-ed with "/kylin/metadata-prefix" automatically.
 */
public interface DistributedLock
{

  /**
   * Returns the client that owns this instance.
   */
  String getClient();

  /**
   * Acquire the lock at given path, non-blocking.
   *
   * @return If the lock is acquired or not.
   */
  boolean lock(String lockPath);

  /**
   * Acquire the lock at given path, block until given timeout.
   *
   * @return If the lock is acquired or not.
   */
  boolean lock(String lockPath, long timeout);

  /**
   * Returns if lock is available at given path.
   */
  boolean isLocked(String lockPath);

  /**
   * Returns if lock is available at given path.
   */
  boolean isLockedByMe(String lockPath);

  /**
   * Returns the owner of a lock path; returns null if the path is not locked by any one.
   */
  String peekLock(String lockPath);

  /**
   * Unlock the lock at given path.
   *
   * @throws IllegalStateException if the client is not holding the lock.
   */
  void unlock(String lockPath) throws IllegalStateException;

  /**
   * Purge all locks under given path. For clean up.
   */
  void purgeLocks(String lockPathRoot);
}
