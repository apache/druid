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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.query.Query;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.segment.column.RowSignature;

public class WindowOperatorQueryKit implements QueryKit<WindowOperatorQuery>
{
  private final ObjectMapper jsonMapper;

  public WindowOperatorQueryKit(ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  @Override
  public QueryDefinition makeQueryDefinition(
      String queryId,
      WindowOperatorQuery originalQuery,
      QueryKit<Query<?>> queryKit,
      ShuffleSpecFactory resultShuffleSpecFactory,
      int maxWorkerCount,
      int minStageNumber
  )
  {
    // need to validate query first

    final QueryDefinitionBuilder queryDefBuilder = QueryDefinition.builder().queryId(queryId);
    final DataSourcePlan dataSourcePlan = DataSourcePlan.forDataSource(
        queryKit,
        queryId,
        originalQuery.context(),
        originalQuery.getDataSource(),
        originalQuery.getQuerySegmentSpec(),
        originalQuery.getFilter(),
        null,
        maxWorkerCount,
        minStageNumber,
        false
    );


    dataSourcePlan.getSubQueryDefBuilder().ifPresent(queryDefBuilder::addAll);

    final int firstStageNumber = Math.max(minStageNumber, queryDefBuilder.getNextStageNumber());
    final WindowOperatorQuery queryToRun = (WindowOperatorQuery) originalQuery.withDataSource(dataSourcePlan.getNewDataSource());
    RowSignature rowSignature = queryToRun.getRowSignature();

    // Create a new stage which takes in the subquery as an input
    queryDefBuilder.add(
        StageDefinition.builder(firstStageNumber)
                       .inputs(new StageInputSpec(firstStageNumber - 1))
                       .signature(rowSignature)
                       .maxWorkerCount(maxWorkerCount)
                       .shuffleSpec(null)
                       .processorFactory(new WindowOperatorQueryFrameProcessorFactory(queryToRun))
    );
    return queryDefBuilder.queryId(queryId).build();
  }
}
