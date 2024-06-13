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

package org.apache.druid.concurrent;

public class TaskThreadPriority
{
  // The task context key to grab the task priority from
  public static final String CONTEXT_KEY = "backgroundThreadPriority";
  // NOTE: Setting negative nice values on linux systems (threadPriority > Thread.NORM_PRIORITY) requires running
  // as *ROOT*. This is, in general, not advisable.
  // In order to have these priorities honored on linux systems, the JVM must be launched with the following options:
  //
  //      -XX:+UseThreadPriorities -XX:ThreadPriorityPolicy=42
  //
  // +UseThreadPriorities usually only enables setting thread priorities if run as root... but
  // ThreadPriorityPolicy is "supposed" to be either 0 or 1, but there is a "bug"/feature in
  // the common JVMs. Whereby if UseThreadPriorities is set, and the  ThreadPriorityPolicy!=1
  // the check for "root" is skipped. This works fine as long as you are LOWERING the
  // threadPriority of tasks (which we are). If you modify the code to allow higher priorities
  // things will crash and burn at runtime. It is advisable to set it to 42 so that relevant searches can be found on
  // the flag
  //
  // Not setting these JVM options disables thread priorities on linux systems
  //
  // See : http://www.akshaal.info/2008/04/javas-thread-priorities-in-linux.html for an explanation
  // See : http://hg.openjdk.java.net/jdk8u/jdk8u/hotspot/file/b0c7e7f1bbbe/src/os/linux/vm/os_linux.cpp#l3933 for
  // the bug in action
  // See: https://docs.oracle.com/cd/E15289_01/doc.40/e15062/optionxx.htm#BABGBFHF for the options documentation

  /**
   * Return the thread-factory friendly priorities from the task priority
   *
   * @param taskPriority The priority for the task. >0 means high. 0 means inherit from current thread, <0 means low.
   *
   * @return The thread priority to use in a thread factory, or null if no priority is to be set
   */
  public static Integer getThreadPriorityFromTaskPriority(final int taskPriority)
  {
    if (taskPriority == 0) {
      return null;
    }
    int finalPriority = taskPriority + Thread.NORM_PRIORITY;
    if (taskPriority > Thread.MAX_PRIORITY) {
      return Thread.MAX_PRIORITY;
    }
    if (finalPriority < Thread.MIN_PRIORITY) {
      return Thread.MIN_PRIORITY;
    }
    return finalPriority;
  }
}
