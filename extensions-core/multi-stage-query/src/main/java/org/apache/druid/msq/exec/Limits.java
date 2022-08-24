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

public class Limits
{
  /**
   * Maximum number of columns that can appear in a frame signature.
   *
   * Somewhat less than {@link WorkerMemoryParameters#STANDARD_FRAME_SIZE} divided by typical minimum column size:
   * {@link org.apache.druid.frame.allocation.AppendableMemory#DEFAULT_INITIAL_ALLOCATION_SIZE}.
   */
  public static final int MAX_FRAME_COLUMNS = 2000;

  /**
   * Maximum number of workers that can be used in a stage, regardless of available memory.
   */
  public static final int MAX_WORKERS = 1000;

  /**
   * Maximum number of input files per worker
   */
  public static final int MAX_INPUT_FILES_PER_WORKER = 10_000;

  /**
   * Maximum number of parse exceptions with their stack traces a worker can send to the controller.
   */
  public static final long MAX_VERBOSE_PARSE_EXCEPTIONS = 5;

  /**
   * Maximum number of warnings with their stack traces a worker can send to the controller.
   */
  public static final long MAX_VERBOSE_WARNINGS = 10;

  /**
   * Maximum number of input bytes per worker in case number of tasks is determined automatically.
   */
  public static final long MAX_INPUT_BYTES_PER_WORKER = 10 * 1024 * 1024 * 1024L;

  /**
   * Maximum size of the kernel manipulation queue in {@link org.apache.druid.msq.indexing.MSQControllerTask}.
   */
  public static final int MAX_KERNEL_MANIPULATION_QUEUE_SIZE = 100_000;
}
