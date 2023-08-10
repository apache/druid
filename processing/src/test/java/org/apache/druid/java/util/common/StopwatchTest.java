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

import com.google.common.testing.FakeTicker;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class StopwatchTest
{

  @Test
  public void testDuplicateStartThrowsException()
  {
    Stopwatch stopwatch = Stopwatch.createStarted();
    Assert.assertThrows(IllegalStateException.class, stopwatch::start);
  }

  @Test
  public void testDuplicateStopThrowsException()
  {
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    Assert.assertThrows(IllegalStateException.class, stopwatch::stop);
  }

  @Test
  public void testMillisElapsed()
  {
    FakeTicker fakeTicker = new FakeTicker();
    Stopwatch stopwatch = Stopwatch.createStarted(fakeTicker);
    fakeTicker.advance(100, TimeUnit.MILLISECONDS);
    stopwatch.stop();

    Assert.assertEquals(100, stopwatch.millisElapsed());
  }

  @Test
  public void testHasElapsed()
  {
    FakeTicker fakeTicker = new FakeTicker();
    Stopwatch stopwatch = Stopwatch.createStarted(fakeTicker);
    fakeTicker.advance(100, TimeUnit.MILLISECONDS);
    stopwatch.stop();

    Assert.assertTrue(stopwatch.hasElapsed(Duration.millis(50)));
    Assert.assertTrue(stopwatch.hasElapsed(Duration.millis(100)));
    Assert.assertTrue(stopwatch.hasNotElapsed(Duration.millis(101)));
    Assert.assertTrue(stopwatch.hasNotElapsed(Duration.millis(500)));
  }
}
