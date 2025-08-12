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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.query.QueryContext;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Multi-stage query definition.
 *
 * Query definitions are a directed acyclic graph (DAG) of stages, represented by {@link StageDefinition}.
 *
 * One stage is the final stage, which could also be called the root stage, and which produces the output for
 * the query. Queries have a single final stage: in other words, they do not have multiple outputs.
 */
public class QueryDefinition
{
  private final Map<StageId, StageDefinition> stageDefinitions;
  private final StageId finalStage;
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private final QueryContext context;

  private QueryDefinition(
      final Map<StageId, StageDefinition> stageDefinitions,
      final StageId finalStage,
      final QueryContext context
  )
  {
    this.stageDefinitions = stageDefinitions;
    this.finalStage = finalStage;
    this.context = context;
  }

  @JsonCreator
  public static QueryDefinition create(
      @JsonProperty("stages") final List<StageDefinition> stageDefinitions,
      @Nullable @JsonProperty("context") final QueryContext context)
  {
    final Map<StageId, StageDefinition> stageMap = new HashMap<>();
    final Set<StageId> nonFinalStages = new HashSet<>();
    final IntSet stageNumbers = new IntOpenHashSet();

    for (final StageDefinition stage : stageDefinitions) {
      if (!stageNumbers.add(stage.getStageNumber())) {
        throw new ISE("Cannot accept duplicate stage numbers");
      }

      stageMap.put(stage.getId(), stage);

      for (int stageNumber : stage.getInputStageNumbers()) {
        nonFinalStages.add(new StageId(stage.getId().getQueryId(), stageNumber));
      }
    }

    for (final StageId nonFinalStageId : nonFinalStages) {
      if (!stageMap.containsKey(nonFinalStageId)) {
        throw new ISE("Stage [%s] is missing a definition", nonFinalStageId);
      }
    }

    final int finalStageCandidates = stageMap.size() - nonFinalStages.size();

    if (finalStageCandidates == 1) {

      return new QueryDefinition(
          stageMap,
          Iterables.getOnlyElement(Sets.difference(stageMap.keySet(), nonFinalStages)),
          context == null ? QueryContext.empty() : context
      );
    } else {
      throw new IAE("Must have a single final stage, but found [%d] candidates", finalStageCandidates);
    }
  }

  public static QueryDefinitionBuilder builder(final String queryId)
  {
    return new QueryDefinitionBuilder(queryId);
  }

  public static QueryDefinitionBuilder builder(final QueryDefinition queryDef)
  {
    return new QueryDefinitionBuilder(queryDef.getQueryId()).addAll(queryDef);
  }

  public String getQueryId()
  {
    return finalStage.getQueryId();
  }

  public StageDefinition getFinalStageDefinition()
  {
    return getStageDefinition(finalStage);
  }

  @JsonProperty("stages")
  public List<StageDefinition> getStageDefinitions()
  {
    return ImmutableList.copyOf(stageDefinitions.values());
  }

  @JsonProperty("context")
  public QueryContext getContext()
  {
    return context;
  }

  public StageDefinition getStageDefinition(final int stageNumber)
  {
    return getStageDefinition(new StageId(getQueryId(), stageNumber));
  }

  public StageDefinition getStageDefinition(final StageId stageId)
  {
    return Preconditions.checkNotNull(stageDefinitions.get(stageId), "No stageId [%s]", stageId);
  }

  /**
   * Returns a number that is higher than all current stage numbers.
   */
  public int getNextStageNumber()
  {
    return stageDefinitions.values().stream().mapToInt(StageDefinition::getStageNumber).max().orElse(-1) + 1;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryDefinition that = (QueryDefinition) o;
    return Objects.equals(stageDefinitions, that.stageDefinitions) &&
        Objects.equals(finalStage, that.finalStage) &&
        Objects.equals(context, that.context);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stageDefinitions, finalStage, context);
  }

  @Override
  public String toString()
  {
    return "QueryDefinition{" +
        "context=" + context + "," +
        "stageDefinitions=" + stageDefinitions +
        '}';
  }

  /**
   * Returns the final stage's output signature.
   *
   * This signature should align with the plan's output signature - and it should be free from helper columns.
   */
  public RowSignature getOutputRowSignature()
  {
    RowSignature signature = stageDefinitions.get(finalStage).getSignature();
    List<String> names = new ArrayList<>(signature.getColumnNames());
    names.remove(QueryKitUtils.PARTITION_BOOST_COLUMN);
    return signature.buildSafeSignature(names);
  }

  public QueryDefinition withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new QueryDefinition(stageDefinitions, finalStage, context.override(contextOverride));
  }
}
