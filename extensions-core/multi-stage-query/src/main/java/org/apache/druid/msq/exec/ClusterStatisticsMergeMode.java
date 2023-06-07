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
 * If a query requires key statistics to generate partition boundaries, key statistics are gathered by the workers while
 * reading rows from the datasource. These statistics must be transferred to the controller to be merged together.
 * The modes below dictates how {@link WorkerSketchFetcher} gets sketches for the partition boundaries from workers.
 */
public enum ClusterStatisticsMergeMode
{
  /**
   * Fetches the sketches in ascending order of time and generates the partition boundaries for one time
   * chunk at a time. This gives more working memory to the controller for merging sketches, which results in less
   * down sampling and thus, more accuracy. There is, however, a time overhead on fetching sketches in sequential order. This is
   * good for cases where accuracy is important.
   */
  SEQUENTIAL,

  /**
   * Fetche the key statistics for all time chunks from all workers together. The controller then downsamples
   * the sketch if it does not fit in memory. This is faster than `SEQUENTIAL` mode as there is less over head in fetching sketches
   * for all time chunks together. This is good for small sketches which won't be down sampled even if merged together or if
   * accuracy in segment sizing for the ingestion is not very important.
   */
  PARALLEL,

  /**
   * Tries to decide between sequential and parallel modes based on the number of workers and size of the input.
   * <p>
   * If there are more than 100 workers or if the combined sketch size among all workers is more than
   * 1,000,000,000 bytes, SEQUENTIAL mode is chosen, otherwise, PARALLEL mode is chosen.
   */
  AUTO
}
