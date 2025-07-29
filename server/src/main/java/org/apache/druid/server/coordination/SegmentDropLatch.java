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

package org.apache.druid.server.coordination;

import com.google.errorprone.annotations.concurrent.GuardedBy;

/**
 * Latch held by {@link SegmentLoadDropHandler#segmentDropLatches} when a drop is scheduled or actively happening.
 */
public class SegmentDropLatch
{
  enum State
  {
    PENDING,
    DROPPING,
    DONE
  }

  private final Object lock = new Object();

  @GuardedBy("lock")
  private State state = State.PENDING;

  /**
   * Sets this latch to {@link State#DROPPING} state, if it was in {@link State#PENDING}.
   *
   * @return whether the original state was {@link State#PENDING}
   */
  public boolean startDropping()
  {
    synchronized (lock) {
      if (state == State.PENDING) {
        state = State.DROPPING;
        lock.notifyAll();
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Sets this latch to {@link State#DONE} state, if it was in {@link State#DROPPING}. Otherwise does nothing.
   */
  public void doneDropping()
  {
    synchronized (lock) {
      if (state == State.DROPPING) {
        state = State.DONE;
        lock.notifyAll();
      }
    }
  }

  /**
   * Cancels this latch if the drop has not yet begun to execute. Otherwise, waits for the drop to finish.
   * Once this method returns, the drop is definitely no longer scheduled (it has either been canceled, or has
   * already happened).
   */
  public void cancelOrAwait() throws InterruptedException
  {
    synchronized (lock) {
      if (state == State.PENDING) {
        state = State.DONE;
      } else {
        while (state != State.DONE) {
          lock.wait();
        }
      }
    }
  }
}
