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

package org.apache.druid.frame.allocation;

import org.apache.datasketches.memory.Memory;

/**
 * Reference to a particular region of some {@link Memory}. This is used because it is cheaper to create than
 * calling {@link Memory#region}.
 *
 * Not immutable. The pointed-to range may change as this object gets reused.
 */
public class MemoryRange<T extends Memory>
{
  private T memory;
  private long start;
  private long length;

  public MemoryRange(T memory, long start, long length)
  {
    set(memory, start, length);
  }

  /**
   * Returns the underlying memory *without* clipping it to this particular range. Callers must remember to continue
   * applying the offset given by {@link #start} and capacity given by {@link #length}.
   */
  public T memory()
  {
    return memory;
  }

  public long start()
  {
    return start;
  }

  public long length()
  {
    return length;
  }

  public void set(final T memory, final long start, final long length)
  {
    this.memory = memory;
    this.start = start;
    this.length = length;
  }
}
