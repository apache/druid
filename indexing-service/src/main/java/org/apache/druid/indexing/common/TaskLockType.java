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

package org.apache.druid.indexing.common;

/**
 * 1) A revoked lock is compatible with every lock.
 * 2) An appending task may use only EXCLUSIVE, SHARED or APPEND locks
 * 3) A replacing task may use only EXCLUSIVE or REPLACE locks
 * 4) REPLACE and APPEND locks can only be used with timechunk locking
 */
public enum TaskLockType
{
  /**
   * There can be at most one active EXCLUSIVE lock for a given interval.
   * It cannot co-exist with any other active locks with overlapping intervals.
   */
  EXCLUSIVE,
  /**
   * There can be any number of active SHARED locks for a given interval.
   * They can coexist only with other SHARED locks, but not with active locks of other types.
   */
  SHARED,
  /**
   * There can be at most one active REPLACE lock for a given interval.
   * It can co-exist only with APPEND locks whose intervals are enclosed within its interval,
   * and is incompatible with all other active locks with overlapping intervals.
   */
  REPLACE,
  /**
   * There can be any number of active APPEND locks for a given interval.
   * They can coexist with other APPEND locks,
   * and with at most one REPLACE lock whose interval encloses that of the APPEND lock.
   * They are incompatible with all other active locks.
   */
  APPEND
}
