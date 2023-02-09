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

import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.query.Query;

/**
 * Kit for creating multi-stage query {@link QueryDefinition} out of Druid native {@link Query} objects.
 */
public interface QueryKit<QueryType extends Query<?>>
{
  /**
   * Creates a {@link QueryDefinition} from a {@link Query}.
   *
   * @param queryId                  query ID of the resulting {@link QueryDefinition}
   * @param query                    native query to translate
   * @param toolKitForSubQueries     kit that is used to translate native subqueries; i.e.,
   *                                 {@link org.apache.druid.query.QueryDataSource}. Typically a {@link MultiQueryKit}.
   * @param resultShuffleSpecFactory shuffle spec factory for the final output of this query.
   * @param maxWorkerCount           maximum number of workers: becomes
   *                                 {@link org.apache.druid.msq.kernel.StageDefinition#getMaxWorkerCount()}
   * @param minStageNumber           lowest stage number to use for any generated stages. Useful if the resulting
   *                                 {@link QueryDefinition} is going to be added to an existing
   *                                 {@link org.apache.druid.msq.kernel.QueryDefinitionBuilder}.
   */
  QueryDefinition makeQueryDefinition(
      String queryId,
      QueryType query,
      QueryKit<Query<?>> toolKitForSubQueries,
      ShuffleSpecFactory resultShuffleSpecFactory,
      int maxWorkerCount,
      int minStageNumber
  );
}
