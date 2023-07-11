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

package org.apache.druid.query.groupby.epinephelinae.collection;

import org.apache.datasketches.memory.Memory;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;

/**
 * Reusable pointer to a location in {@link Memory}. Allows returning slices of memory without using
 * {@link Memory#region}, which leads to allocations.
 */
public class MemoryPointer
{
  @Nullable
  private Memory memory;
  private long position;

  public MemoryPointer()
  {
    this(null, 0);
  }

  public MemoryPointer(@Nullable Memory memory, long position)
  {
    this.memory = memory;
    this.position = position;
  }

  public Memory memory()
  {
    if (memory == null) {
      throw new ISE("Memory not set");
    }

    return memory;
  }

  public long position()
  {
    return position;
  }

  public void set(final Memory memory, final long position)
  {
    this.memory = memory;
    this.position = position;
  }
}
