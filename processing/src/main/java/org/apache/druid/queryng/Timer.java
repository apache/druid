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

package org.apache.druid.queryng;

/**
 * Very simple nano-second timer with an on/off switch.
 */
public class Timer
{
  private long totalTime;
  private long startTime;

  public static Timer create()
  {
    return new Timer();
  }

  public static Timer createStarted()
  {
    Timer timer = create();
    timer.start();
    return timer;
  }

  public static Timer createAt(long timeNs)
  {
    Timer timer = create();
    timer.startTime = timeNs;
    return timer;
  }

  public void start()
  {
    if (startTime == 0) {
      startTime = System.nanoTime();
    }
  }

  public void stop()
  {
    if (startTime != 0) {
      totalTime += System.nanoTime() - startTime;
      startTime = 0;
    }
  }

  public long get()
  {
    stop();
    return totalTime;
  }
}
