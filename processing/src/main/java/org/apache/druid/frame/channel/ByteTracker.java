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

package org.apache.druid.frame.channel;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.query.ResourceLimitExceededException;


public class ByteTracker
{
  @GuardedBy("this")
  private final long maxBytes;

  @GuardedBy("this")
  private long currentBytes;

  public ByteTracker(long maxBytes)
  {
    this.maxBytes = maxBytes;
  }

  public synchronized void reserve(long byteCount) throws ResourceLimitExceededException
  {
    Preconditions.checkState(byteCount >= 0, "Can't reserve negative bytes");
    if (currentBytes + byteCount > maxBytes) {
      throw new ResourceLimitExceededException("");
    }
    currentBytes += byteCount;
  }

  public synchronized void release(long byteCount)
  {
    Preconditions.checkState(byteCount >= 0, "Can't release negative bytes");
    Preconditions.checkState(currentBytes >= byteCount, "Can't release more than used bytes");
    currentBytes -= byteCount;
  }
}
