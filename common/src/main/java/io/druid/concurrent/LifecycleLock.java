/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.concurrent;

/**
 */
public class LifecycleLock
{
  boolean started = false;
  /**
   * Start latch, only one canStart() call in any thread on this LifecycleLock object could return true.
   */
  public boolean canStart()
  {
    return started == false;
  }

  /**
   * Announce the start was successful.
   *
   * @throws IllegalMonitorStateException if {@link #canStart()} is not yet called or if {@link #exitStart()} is already
   * called on this LifecycleLock
   */
  public void started()
  {
    started = true;
  }

  /**
   * Must be called before exit from start() on the lifecycled object, usually in a finally block.
   *
   * @throws IllegalMonitorStateException if {@link #canStart()} is not yet called on this LifecycleLock
   */
  public void exitStart()
  {
  }

  /**
   * Awaits until {@link #exitStart()} is called, if needed, and returns {@code true} if {@link #started()} was called
   * before that.
   */
  public boolean isStarted()
  {
    return started;
  }

  /**
   * Stop latch, only one canStop() call in any thread on this LifecycleLock object could return {@code true}. If
   * {@link #started()} wasn't called on this LifecycleLock object, always returns {@code false}.
   *
   * @throws IllegalMonitorStateException if {@link #canStart()} and {@link #exitStart()} are not yet called on this
   * LifecycleLock
   */
  public boolean canStop()
  {
    if(started) {
      started = false;
      return true;
    }
    return false;
  }
}
