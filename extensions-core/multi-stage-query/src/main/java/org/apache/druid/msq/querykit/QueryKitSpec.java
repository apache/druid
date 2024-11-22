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

package org.apache.druid.msq.querykit;

import org.apache.druid.msq.input.InputSpecs;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.query.Query;

/**
 * Collection of parameters for {@link QueryKit#makeQueryDefinition}.
 */
public class QueryKitSpec
{
  private final QueryKit<Query<?>> queryKit;
  private final String queryId;
  private final int maxLeafWorkerCount;
  private final int maxNonLeafWorkerCount;
  private final int targetPartitionsPerWorker;

  /**
   * @param queryKit                  kit that is used to translate native subqueries; i.e.,
   *                                  {@link org.apache.druid.query.QueryDataSource}. Typically a {@link MultiQueryKit}.
   * @param queryId                   queryId of the resulting {@link QueryDefinition}
   * @param maxLeafWorkerCount        maximum number of workers for leaf stages: becomes
   *                                  {@link StageDefinition#getMaxWorkerCount()}
   * @param maxNonLeafWorkerCount     maximum number of workers for non-leaf stages: becomes
   *                                  {@link StageDefinition#getMaxWorkerCount()}
   * @param targetPartitionsPerWorker preferred number of partitions per worker for subqueries
   */
  public QueryKitSpec(
      QueryKit<Query<?>> queryKit,
      String queryId,
      int maxLeafWorkerCount,
      int maxNonLeafWorkerCount,
      int targetPartitionsPerWorker
  )
  {
    this.queryId = queryId;
    this.queryKit = queryKit;
    this.maxLeafWorkerCount = maxLeafWorkerCount;
    this.maxNonLeafWorkerCount = maxNonLeafWorkerCount;
    this.targetPartitionsPerWorker = targetPartitionsPerWorker;
  }

  /**
   * Instance of {@link QueryKit} for recursive calls.
   */
  public QueryKit<Query<?>> getQueryKit()
  {
    return queryKit;
  }

  /**
   * Query ID to use when building {@link QueryDefinition}.
   */
  public String getQueryId()
  {
    return queryId;
  }

  /**
   * Maximum number of workers for leaf stages. See {@link InputSpecs#hasLeafInputs}.
   */
  public int getMaxLeafWorkerCount()
  {
    return maxLeafWorkerCount;
  }

  /**
   * Maximum number of workers for non-leaf stages. See {@link InputSpecs#hasLeafInputs}.
   */
  public int getMaxNonLeafWorkerCount()
  {
    return maxNonLeafWorkerCount;
  }

  /**
   * Number of partitions to generate during a shuffle.
   */
  public int getNumPartitionsForShuffle()
  {
    return maxNonLeafWorkerCount * targetPartitionsPerWorker;
  }
}
