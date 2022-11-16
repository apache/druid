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
 * Mode which dictates how {@link WorkerSketchFetcher} gets sketches for the partition boundaries from workers.
 */
public enum ClusterStatisticsMergeMode
{
  /**
   * Fetches sketch in sequential order based on time. Slower due to overhead, but more accurate.
   */
  SEQUENTIAL,

  /**
   * Fetch all sketches from the worker at once. Faster to generate partitions, but less accurate.
   */
  PARALLEL,

  /**
   * Tries to decide between sequential and parallel modes based on the number of workers and size of the input
   *
   * If there are more than 100 workers or if the combined sketch size among all workers is more than
   * 1,000,000,000 bytes, SEQUENTIAL mode is chosen, otherwise, PARALLEL mode is chosen.
   */
  AUTO
}
