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

package org.apache.druid.java.util.metrics;

import java.util.List;
import java.util.Map;

public class Monitors
{
  /**
   * Creates a JVM monitor, configured with the given dimensions, that gathers all currently available JVM-wide
   * monitors. Emitted events have default feed {@link FeedDefiningMonitor#DEFAULT_METRICS_FEED}
   * See: {@link Monitors#createCompoundJvmMonitor(Map, String)}
   *
   * @param dimensions common dimensions to configure the JVM monitor with
   *
   * @return a universally useful JVM-wide monitor
   */
  public static Monitor createCompoundJvmMonitor(Map<String, String[]> dimensions)
  {
    return createCompoundJvmMonitor(dimensions, FeedDefiningMonitor.DEFAULT_METRICS_FEED);
  }

  /**
   * Creates a JVM monitor, configured with the given dimensions, that gathers all currently available JVM-wide
   * monitors: {@link JvmMonitor}, {@link JvmCpuMonitor} and {@link JvmThreadsMonitor} (this list may
   * change in any future release of this library, including a minor release).
   *
   * @param dimensions common dimensions to configure the JVM monitor with
   * @param feed       feed for all emitted events
   *
   * @return a universally useful JVM-wide monitor
   */
  public static Monitor createCompoundJvmMonitor(Map<String, String[]> dimensions, String feed)
  {
    // This list doesn't include SysMonitor because it should probably be run only in one JVM, if several JVMs are
    // running on the same instance, so most of the time SysMonitor should be configured/set up differently than
    // "simple" JVM monitors, created below.
    return and(// Could equally be or(), because all member monitors always return true from their monitor() methods.
                new JvmMonitor(dimensions, feed),
                new JvmCpuMonitor(dimensions, feed),
                new JvmThreadsMonitor(dimensions, feed)
    );
  }

  public static Monitor and(Monitor... monitors)
  {
    return new CompoundMonitor(monitors)
    {
      @Override
      public boolean shouldReschedule(List<Boolean> reschedules)
      {
        boolean b = true;
        for (boolean reschedule : reschedules) {
          b = b && reschedule;
        }
        return b;
      }
    };
  }
}
