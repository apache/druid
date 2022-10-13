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

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Builder for {@link QueryDefinition}.
 */
public class QueryDefinitionBuilder
{
  private String queryId = UUID.randomUUID().toString();
  private final List<StageDefinitionBuilder> stageBuilders = new ArrayList<>();

  /**
   * Package-private: callers should use {@link QueryDefinition#builder()}.
   */
  QueryDefinitionBuilder()
  {
  }

  public QueryDefinitionBuilder queryId(final String queryId)
  {
    this.queryId = Preconditions.checkNotNull(queryId, "queryId");
    return this;
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

  public QueryDefinition build()
  {
    final List<StageDefinition> stageDefinitions =
        stageBuilders.stream().map(builder -> builder.build(queryId)).collect(Collectors.toList());

    return QueryDefinition.create(stageDefinitions);
  }
}
