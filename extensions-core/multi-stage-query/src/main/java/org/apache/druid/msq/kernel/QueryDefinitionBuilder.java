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

package org.apache.druid.msq.kernel;

import org.apache.druid.java.util.common.ISE;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builder for {@link QueryDefinition}.
 */
public class QueryDefinitionBuilder
{
  private final String queryId;
  private final List<StageDefinitionBuilder> stageBuilders = new ArrayList<>();

  /**
   * Package-private: callers should use {@link QueryDefinition#builder(String)}.
   */
  QueryDefinitionBuilder(final String queryId)
  {
    this.queryId = queryId;
  }

  public QueryDefinitionBuilder add(final StageDefinitionBuilder stageBuilder)
  {
    stageBuilders.add(stageBuilder);
    return this;
  }

  public QueryDefinitionBuilder addAll(final QueryDefinition queryDef)
  {
    for (final StageDefinition stageDef : queryDef.getStageDefinitions()) {
      add(StageDefinition.builder(stageDef));
    }
    return this;
  }

  public QueryDefinitionBuilder addAll(final QueryDefinitionBuilder queryDefBuilder)
  {
    for (final StageDefinitionBuilder stageDefBuilder : queryDefBuilder.stageBuilders) {
      add(stageDefBuilder);
    }
    return this;
  }

  /**
   * Returns a number that is higher than all current stage numbers.
   */
  public int getNextStageNumber()
  {
    return stageBuilders.stream().mapToInt(StageDefinitionBuilder::getStageNumber).max().orElse(-1) + 1;
  }

  public StageDefinitionBuilder getStageBuilder(final int stageNumber)
  {
    for (final StageDefinitionBuilder stageBuilder : stageBuilders) {
      if (stageBuilder.getStageNumber() == stageNumber) {
        return stageBuilder;
      }
    }

    throw new ISE("No such stage [%s]", stageNumber);
  }

  public QueryDefinition build()
  {
    final List<StageDefinition> stageDefinitions =
        stageBuilders.stream().map(builder -> builder.build(queryId)).collect(Collectors.toList());

    return QueryDefinition.create(stageDefinitions);
  }
}
