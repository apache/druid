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

package org.apache.druid.segment.realtime.appenderator;

import org.joda.time.Interval;

import java.io.IOException;

/**
 * Used to release task locks after segments have been handed off, typically with long-running tasks
 * to avoid holding locks for longer than necessary. This interface is used instead of {@code TaskActionClient}
 * to prevent a cyclic dependency with druid-indexing-service module.
 */
@FunctionalInterface
public interface TaskIntervalUnlocker
{
  /**
   * Releases the lock for the exact interval for a task.
   */
  void releaseLock(Interval interval) throws IOException;
}
