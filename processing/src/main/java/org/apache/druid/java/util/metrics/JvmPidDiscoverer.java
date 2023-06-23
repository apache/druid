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

import org.apache.druid.java.util.common.RE;

import java.lang.management.ManagementFactory;
import java.util.regex.Pattern;

/**
 * For systems that for whatever reason cannot use Sigar (through org.apache.druid.java.util.metrics.SigarPidDiscoverer ),
 * this attempts to get the PID from the JVM "name".
 */
public class JvmPidDiscoverer implements PidDiscoverer
{
  private static final JvmPidDiscoverer INSTANCE = new JvmPidDiscoverer();

  public static JvmPidDiscoverer instance()
  {
    return INSTANCE;
  }

  /**
   * use {JvmPidDiscoverer.instance()}
   */
  private JvmPidDiscoverer()
  {
  }

  /**
   * Returns the PID as a best guess. This uses methods that are not guaranteed to actually be the PID.
   * <p>
   * TODO: switch to ProcessHandle.current().getPid() for java9 potentially
   *
   * @return the PID of the current jvm if available
   *
   * @throws RuntimeException if the pid cannot be determined
   */
  @Override
  public long getPid()
  {
    return Inner.PID;
  }

  private static class Inner
  {
    private static final long PID;

    static {
      final String jvmName = ManagementFactory.getRuntimeMXBean().getName();
      final String[] nameSplits = jvmName.split(Pattern.quote("@"));
      if (nameSplits.length != 2) {
        throw new RE("Unable to determine pid from [%s]", jvmName);
      }
      try {
        PID = Long.parseLong(nameSplits[0]);
      }
      catch (NumberFormatException nfe) {
        throw new RE(nfe, "Unable to determine pid from [%s]", jvmName);
      }
    }
  }
}
