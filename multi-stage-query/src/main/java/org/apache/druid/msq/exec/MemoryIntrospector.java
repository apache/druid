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

package org.apache.druid.msq.exec;

/**
 * Introspector used to generate {@link WorkerMemoryParameters} and {@link ControllerMemoryParameters}.
 */
public interface MemoryIntrospector
{
  /**
   * Amount of total memory in the entire JVM.
   */
  long totalMemoryInJvm();

  /**
   * Amount of memory alloted to each {@link Worker} or {@link Controller}.
   */
  long memoryPerTask();

  /**
   * Computes the amount of total JVM memory that would be required for a particular memory allotment per task, i.e.,
   * a particular return value from {@link #memoryPerTask()}.
   */
  long computeJvmMemoryRequiredForTaskMemory(long memoryPerTask);

  /**
   * Maximum number of tasks ({@link Worker} or {@link Controller}) that run simultaneously in this JVM.
   */
  int numTasksInJvm();

  /**
   * Maximum number of processing threads that can be used at once by each {@link Worker} or {@link Controller}.
   */
  int numProcessingThreads();
}
