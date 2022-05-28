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

package org.apache.druid.java.util.common.concurrent;

import org.joda.time.Duration;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;

public class ScheduledExecutorsTest
{
  @Test
  public void testscheduleWithFixedDelay() throws InterruptedException
  {
    Duration initialDelay = new Duration(1000);
    Duration delay = new Duration(1000);
    ScheduledExecutorService exec = Execs.scheduledSingleThreaded("BasicAuthenticatorCacheManager-Exec--%d");
    ScheduledExecutors.scheduleWithFixedDelay(
            exec,
            initialDelay,
            delay,
        () -> {
            System.out.println("TEST!");
        }
    );
    Thread.sleep(5 * 1000);
    exec.shutdown();
  }
}
