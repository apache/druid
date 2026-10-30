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

package org.apache.druid.testing.embedded.junit5;

import io.netty.util.ResourceLeakDetector;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A Netty {@link ResourceLeakDetector} that records every leak it observes and installs a JVM
 * shutdown hook that fails the test fork (non-zero exit) if any leak was recorded. Netty's default
 * detector only logs leaks, and the log line is easily missed in test output; surefire treats a
 * non-zero fork exit as a failure, so this wires ByteBuf leaks straight into the build result.
 *
 * <p>Enable by adding {@code -Dio.netty.customResourceLeakDetector} pointing at this class alongside
 * {@code -Dio.netty.leakDetection.level=PARANOID}. Netty instantiates the class reflectively via the
 * {@code (Class<?>, int)} constructor.
 */
public class FailFastLeakDetector<T> extends ResourceLeakDetector<T>
{
  private static final List<String> LEAKS = new CopyOnWriteArrayList<>();
  private static final int EXIT_CODE = 66;

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(FailFastLeakDetector::maybeFailJvmOnLeaks, "netty-leak-check"));
  }

  public FailFastLeakDetector(Class<?> resourceType, int samplingInterval)
  {
    super(resourceType, samplingInterval);
  }

  @Override
  protected void reportTracedLeak(String resourceType, String records)
  {
    super.reportTracedLeak(resourceType, records);
    LEAKS.add("LEAK: " + resourceType + records);
  }

  @Override
  protected void reportUntracedLeak(String resourceType)
  {
    super.reportUntracedLeak(resourceType);
    LEAKS.add("LEAK (untraced): " + resourceType);
  }

  private static void maybeFailJvmOnLeaks()
  {
    // Give any unreachable-but-unreported buffers a nudge; ResourceLeakDetector reports pending leaks
    // as a side effect of subsequent track()/close() calls, which stop happening at shutdown, so this
    // is best-effort. Anything reported during the test run is already in LEAKS regardless.
    for (int i = 0; i < 3; i++) {
      System.gc();
      try {
        Thread.sleep(100);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    if (!LEAKS.isEmpty()) {
      System.err.println("Netty ByteBuf leaks detected in test JVM:");
      for (String leak : LEAKS) {
        System.err.println(leak);
      }
      Runtime.getRuntime().halt(EXIT_CODE);
    }
  }
}
