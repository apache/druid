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

import org.apache.druid.utils.RuntimeInfo;

/**
 * Like {@link RuntimeInfo}, but adjusted based on the number of tasks running in a JVM, so each task gets its
 * own processors and memory. Returned by {@link TaskToolbox#getAdjustedRuntimeInfo()}.
 */
public class AdjustedRuntimeInfo extends RuntimeInfo
{
  private final RuntimeInfo base;
  private final int numTasksInJvm;

  public AdjustedRuntimeInfo(final RuntimeInfo base, final int numTasksInJvm)
  {
    this.base = base;
    this.numTasksInJvm = numTasksInJvm;
  }

  @Override
  public int getAvailableProcessors()
  {
    return Math.max(1, base.getAvailableProcessors() / numTasksInJvm);
  }

  @Override
  public long getMaxHeapSizeBytes()
  {
    return base.getMaxHeapSizeBytes() / numTasksInJvm;
  }

  @Override
  public long getDirectMemorySizeBytes()
  {
    return base.getDirectMemorySizeBytes() / numTasksInJvm;
  }
}
