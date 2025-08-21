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

package org.apache.druid.msq.kernel.controller;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntBooleanPair;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.exec.StageProcessor;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.GlobalSortMaxCountShuffleSpec;
import org.apache.druid.msq.kernel.HashShuffleSpec;
import org.apache.druid.msq.kernel.MixShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.ShuffleKind;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class MockQueryDefinitionBuilder
{
  static final String SHUFFLE_KEY_COLUMN = "shuffleKey";
  static final RowSignature STAGE_SIGNATURE = RowSignature.builder().add(SHUFFLE_KEY_COLUMN, ColumnType.STRING).build();

  private static final int MAX_NUM_PARTITIONS = 32;

  private final int numStages;

  // Maps a stage to all the other stages on which it has dependency, i.e. for an edge like A -> B, the adjacency list
  // would have an entry like B : [ <A, isBroadcast>, ... ]
  private final Map<Integer, Set<IntBooleanPair>> adjacencyList = new HashMap<>();

  // Keeps a collection of those stages that have been already defined
  private final Set<Integer> definedStages = new HashSet<>();

  // Query definition builder corresponding to this mock builder
  private final QueryDefinitionBuilder queryDefinitionBuilder = QueryDefinition.builder(UUID.randomUUID().toString());


  public MockQueryDefinitionBuilder(final int numStages)
  {
    this.numStages = numStages;
  }

  public MockQueryDefinitionBuilder addEdge(final int outVertex, final int inVertex)
  {
    return addEdge(outVertex, inVertex, false);
  }

  public MockQueryDefinitionBuilder addEdge(final int outVertex, final int inVertex, final boolean broadcast)
  {
    Preconditions.checkArgument(
        outVertex < numStages,
        "vertex number can only be from 0 to one less than the total number of stages"
    );

    Preconditions.checkArgument(
        inVertex < numStages,
        "vertex number can only be from 0 to one less than the total number of stages"
    );

    Preconditions.checkArgument(
        !definedStages.contains(inVertex),
        StringUtils.format("%s is already defined, cannot create more connections from it", inVertex)
    );

    Preconditions.checkArgument(
        !definedStages.contains(outVertex),
        StringUtils.format("%s is already defined, cannot create more connections to it", outVertex)
    );

    adjacencyList.computeIfAbsent(inVertex, k -> new HashSet<>()).add(IntBooleanPair.of(outVertex, broadcast));
    return this;
  }

  public MockQueryDefinitionBuilder defineStage(
      int stageNumber,
      @Nullable ShuffleKind shuffleKind,
      int maxWorkers
  )
  {
    Preconditions.checkArgument(
        stageNumber < numStages,
        "stageNumber should be between 0 and total stages - 1"
    );
    Preconditions.checkArgument(
        !definedStages.contains(stageNumber),
        StringUtils.format("%d is already defined", stageNumber)
    );
    definedStages.add(stageNumber);

    ShuffleSpec shuffleSpec = null;

    if (shuffleKind != null) {
      switch (shuffleKind) {
        case GLOBAL_SORT:
          shuffleSpec = new GlobalSortMaxCountShuffleSpec(
              new ClusterBy(
                  ImmutableList.of(
                      new KeyColumn(SHUFFLE_KEY_COLUMN, KeyOrder.ASCENDING)
                  ),
                  0
              ),
              MAX_NUM_PARTITIONS,
              false,
              ShuffleSpec.UNLIMITED
          );
          break;

        case HASH_LOCAL_SORT:
        case HASH:
          shuffleSpec = new HashShuffleSpec(
              new ClusterBy(
                  ImmutableList.of(
                      new KeyColumn(
                          SHUFFLE_KEY_COLUMN,
                          shuffleKind == ShuffleKind.HASH ? KeyOrder.NONE : KeyOrder.ASCENDING
                      )
                  ),
                  0
              ),
              MAX_NUM_PARTITIONS
          );
          break;

        case MIX:
          shuffleSpec = MixShuffleSpec.instance();
          break;
      }

      if (shuffleSpec == null || shuffleKind != shuffleSpec.kind()) {
        throw new ISE("Oops, created an incorrect shuffleSpec[%s] for kind[%s]", shuffleSpec, shuffleKind);
      }
    }

    final List<InputSpec> inputSpecs = new ArrayList<>();
    final IntSet broadcastInputNumbers = new IntOpenHashSet();

    int inputNumber = 0;
    for (final IntBooleanPair pair : adjacencyList.getOrDefault(stageNumber, Collections.emptySet())) {
      inputSpecs.add(new StageInputSpec(pair.leftInt()));
      if (pair.rightBoolean()) {
        broadcastInputNumbers.add(inputNumber);
      }
      inputNumber++;
    }

    if (inputSpecs.isEmpty()) {
      for (int i = 0; i < maxWorkers; i++) {
        inputSpecs.add(new ControllerTestInputSpec());
      }
    }

    queryDefinitionBuilder.add(
        StageDefinition.builder(stageNumber)
                       .inputs(inputSpecs)
                       .broadcastInputs(broadcastInputNumbers)
                       .processor(Mockito.mock(StageProcessor.class))
                       .shuffleSpec(shuffleSpec)
                       .signature(RowSignature.builder().add(SHUFFLE_KEY_COLUMN, ColumnType.STRING).build())
                       .maxWorkerCount(maxWorkers)
    );

    return this;
  }

  public MockQueryDefinitionBuilder defineStage(int stageNumber, @Nullable ShuffleKind shuffleKind)
  {
    return defineStage(stageNumber, shuffleKind, 1);
  }

  public MockQueryDefinitionBuilder defineStage(int stageNumber)
  {
    return defineStage(stageNumber, null);
  }

  public QueryDefinitionBuilder getQueryDefinitionBuilder()
  {
    if (!verifyIfAcyclic()) {
      throw new ISE("The stages of the query form a cycle. Cannot create a query definition builder");
    }
    for (int i = 0; i < numStages; ++i) {
      if (!definedStages.contains(i)) {
        defineStage(i);
      }
    }
    return queryDefinitionBuilder;
  }

  /**
   * Perform a basic check that the query definition that the user is trying to build is acyclic indeed. This method
   * is not required in the source code because the DAG there is created by query toolkits.
   */
  private boolean verifyIfAcyclic()
  {
    Map<Integer, StageState> visited = new HashMap<>();

    for (int i = 0; i < numStages; i++) {
      if (!checkAcyclic(i, visited)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks for graph cycles using DFS
   */
  private boolean checkAcyclic(int node, Map<Integer, StageState> visited)
  {
    StageState state = visited.getOrDefault(node, StageState.NEW);
    if (state == StageState.VISITED) {
      return true;
    }
    if (state == StageState.VISITING) {
      return false;
    } else {
      visited.put(node, StageState.VISITING);
      for (IntBooleanPair neighbour : adjacencyList.getOrDefault(node, Collections.emptySet())) {
        if (!checkAcyclic(neighbour.leftInt(), visited)) {
          return false;
        }
      }
      visited.put(node, StageState.VISITED);
      return true;
    }
  }

  private enum StageState
  {
    NEW,
    VISITING,
    VISITED
  }
}
