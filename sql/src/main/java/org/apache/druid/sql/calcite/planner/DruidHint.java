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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.druid.query.JoinAlgorithm;

import javax.annotation.Nullable;

public abstract class DruidHint
{
  public abstract static class DruidJoinHint extends DruidHint
  {
    @Nullable
    public static DruidJoinHint fromString(String hintName)
    {
      switch (hintName) {
        case BroadcastJoinHint.BROADCAST_JOIN:
          return new BroadcastJoinHint();
        case SortMergeJoinHint.SORT_MERGE_JOIN:
          return new SortMergeJoinHint();
        default:
          return null;
      }
    }

    abstract JoinAlgorithm asJoinAlgorithm();

    abstract String id();

    public static class SortMergeJoinHint extends DruidJoinHint
    {
      static final String SORT_MERGE_JOIN = "sort_merge";

      @Override
      String id()
      {
        return SORT_MERGE_JOIN;
      }

      @Override
      public JoinAlgorithm asJoinAlgorithm()
      {
        return JoinAlgorithm.SORT_MERGE;
      }
    }

    public static class BroadcastJoinHint extends DruidJoinHint
    {
      static final String BROADCAST_JOIN = "broadcast";

      @Override
      String id()
      {
        return BROADCAST_JOIN;
      }

      @Override
      public JoinAlgorithm asJoinAlgorithm()
      {
        return JoinAlgorithm.BROADCAST;
      }
    }
  }

  public static final HintStrategyTable HINT_STRATEGY_TABLE = createHintStrategies();

  /**
   * Creates hint strategies.
   *
   * @return HintStrategyTable instance
   */
  private static HintStrategyTable createHintStrategies()
  {
    return HintStrategyTable.builder()
                            .hintStrategy(DruidJoinHint.SortMergeJoinHint.SORT_MERGE_JOIN, HintPredicates.JOIN)
                            .hintStrategy(DruidJoinHint.BroadcastJoinHint.BROADCAST_JOIN, HintPredicates.JOIN)
                            .build();
  }
}
