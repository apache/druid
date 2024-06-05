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

package org.apache.druid.java.util.common;

import com.google.common.base.Ticker;
import org.joda.time.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Wrapper over {@link com.google.common.base.Stopwatch} to provide some utility
 * methods such as {@link #millisElapsed()}, {@link #restart()}, {@link #hasElapsed(Duration)}.
 */
public class Stopwatch
{
  private final com.google.common.base.Stopwatch delegate;

  public static Stopwatch createStarted()
  {
    return new Stopwatch(com.google.common.base.Stopwatch.createStarted());
  }

  public static Stopwatch createUnstarted()
  {
    return new Stopwatch(com.google.common.base.Stopwatch.createUnstarted());
  }

  public static Stopwatch createStarted(Ticker ticker)
  {
    return new Stopwatch(com.google.common.base.Stopwatch.createStarted(ticker));
  }

  private Stopwatch(com.google.common.base.Stopwatch delegate)
  {
    this.delegate = delegate;
  }

  public void start()
  {
    delegate.start();
  }

  public void stop()
  {
    delegate.stop();
  }

  public void reset()
  {
    delegate.reset();
  }

  /**
   * Invokes {@code reset().start()} on the underlying {@link com.google.common.base.Stopwatch}.
   */
  public void restart()
  {
    delegate.reset().start();
  }

  public boolean isRunning()
  {
    return delegate.isRunning();
  }

  /**
   * Returns the milliseconds elapsed on the stopwatch.
   */
  public long millisElapsed()
  {
    return delegate.elapsed(TimeUnit.MILLISECONDS);
  }

  /**
   * Checks if the given duration has already elapsed on the stopwatch.
   */
  public boolean hasElapsed(Duration duration)
  {
    return millisElapsed() >= duration.getMillis();
  }

  /**
   * Checks that the given duration has not elapsed on the stopwatch. Calling this
   * method is the same as {@code !stopwatch.hasElapsed(duration)}.
   */
  public boolean hasNotElapsed(Duration duration)
  {
    return !hasElapsed(duration);
  }

}
