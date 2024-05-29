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

import org.apache.druid.msq.kernel.WorkOrder;

/**
 * Introspector used to generate {@link ControllerMemoryParameters}.
 */
public interface MemoryIntrospector
{
  /**
   * Amount of total memory in the entire JVM.
   */
  long totalMemoryInJvm();

  /**
   * Amount of memory usable for the multi-stage query engine in the entire JVM.
   *
   * This may be an expensive operation. For example, the production implementation {@link MemoryIntrospectorImpl}
   * estimates size of all lookups as part of computing this value.
   */
  long usableMemoryInJvm();

  /**
   * Amount of total JVM memory required for a particular amount of usable memory to be available.
   *
   * This may be an expensive operation. For example, the production implementation {@link MemoryIntrospectorImpl}
   * estimates size of all lookups as part of computing this value.
   */
  long computeJvmMemoryRequiredForUsableMemory(long usableMemory);

  /**
   * Maximum number of queries that run simultaneously in this JVM.
   *
   * On workers, this is the maximum number of {@link Worker} that run simultaneously in this JVM. See
   * {@link WorkerMemoryParameters} for how memory is divided among and within {@link WorkOrder} run by a worker.
   *
   * On controllers, this is the maximum number of {@link Controller} that run simultaneously. See
   * {@link ControllerMemoryParameters} for how memory is used by controllers.
   */
  int numQueriesInJvm();

  /**
   * Maximum number of processing threads that can be used at once in this JVM.
   */
  int numProcessorsInJvm();
}
