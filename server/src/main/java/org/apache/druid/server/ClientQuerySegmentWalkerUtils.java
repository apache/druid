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

package org.apache.druid.server;

/**
 * Utilities for {@link ClientQuerySegmentWalker}
 */
public class ClientQuerySegmentWalkerUtils
{

  /**
   * Guardrail type on the subquery's results
   */
  public enum SubqueryResultLimit
  {
    /**
     * Subqueries limited by the ROW_LIMIT are materialized and kept as arrays (native java objects) on heap. The
     * walker ensures that the cumulative number of rows of the results of subqueries of the given query donot exceed
     * the limit specified in the context or as the server default
     */
    ROW_LIMIT,

    /**
     * Subqueries limited by the BYTE_LIMIT are materialized as {@link org.apache.druid.frame.Frame}s on heap. Frames
     * depict the byte representation of the subquery results and hence the space consumed by the frames can be trivially
     * fetched. The walker ensures that the cumulative number of rows of the results of subqueries (materialized as
     * Frames in the broker memory) of a given query do not exceed the limit specified in the context or as the server
     * default
     */
    MEMORY_LIMIT
  }

  /**
   * Returns the limit type to be used for a given subquery.
   * It returns MEMORY_LIMIT only if:
   *  1. The user has enabled the 'maxSubqueryBytes' explicitly in the query context or as the server default
   *  2. All the other subqueries in the query so far didn't fall back to ROW_BASED limit due to an error while
   *     executing the query
   * In all the other cases, it returns ROW_LIMIT
   */
  public static SubqueryResultLimit getLimitType(long memoryLimitBytes, boolean cannotMaterializeToFrames)
  {
    if (cannotMaterializeToFrames) {
      return SubqueryResultLimit.ROW_LIMIT;
    }
    if (memoryLimitBytes > 0) {
      return SubqueryResultLimit.MEMORY_LIMIT;
    }
    return SubqueryResultLimit.ROW_LIMIT;
  }
}
