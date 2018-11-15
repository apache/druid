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
package org.apache.druid.query;

import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Function;

public final class ParallelCombines
{
  /**
   * Recursively build a combining tree in a bottom-up manner.  Each node of the tree is a task that combines input
   * iterators asynchronously.
   *
   * @param leafChildren              all child nodes at the leaf level
   * @param leafCombineDegree         combining degree for the leaf level
   * @param intermediateCombineDegree combining degree for intermediate levels
   * @param executorSubmitter         function to submit a combining task to the executor
   *
   * @return a pair of a root of the combining tree and a list of futures of all executed combining tasks
   */
  public static <T> Pair<T, List<Future>> buildCombineTree(
      List<T> leafChildren,
      int leafCombineDegree,
      int intermediateCombineDegree,
      Function<List<T>, Pair<T, Future>> executorSubmitter
  )
  {
    final Pair<List<T>, List<Future>> rootAndFutures = buildCombineTree(
        leafChildren,
        leafCombineDegree,
        intermediateCombineDegree,
        executorSubmitter,
        true
    );
    return Pair.of(
        Iterables.getOnlyElement(rootAndFutures.lhs),
        rootAndFutures.rhs
    );
  }

  private static <T> Pair<List<T>, List<Future>> buildCombineTree(
      List<T> children,
      int leafCombineDegree,
      int intermediateCombineDegree,
      Function<List<T>, Pair<T, Future>> executorSubmitter,
      boolean leaf
  )
  {
    final int numChildLevelIterators = children.size();
    final List<T> childIteratorsOfNextLevel = new ArrayList<>();
    final List<Future> combineFutures = new ArrayList<>();
    final int combineDegree = leaf ? leafCombineDegree : intermediateCombineDegree;

    // The below algorithm creates the combining nodes of the current level. It first checks that the number of children
    // to be combined together is 1. If it is, the intermediate combining node for that child is not needed. Instead, it
    // can be directly connected to a node of the parent level. Here is an example of generated tree when
    // numLeafNodes = 6 and leafCombineDegree = intermediateCombineDegree = 2. See the description of
    // MINIMUM_LEAF_COMBINE_DEGREE for more details about leafCombineDegree and intermediateCombineDegree.
    //
    //      o
    //     / \
    //    o   \
    //   / \   \
    //  o   o   o
    // / \ / \ / \
    // o o o o o o
    //
    // We can expect that the aggregates can be combined as early as possible because the tree is built in a bottom-up
    // manner.

    for (int i = 0; i < numChildLevelIterators; i += combineDegree) {
      if (i < numChildLevelIterators - 1) {
        final List<T> subChildren = children.subList(
            i,
            Math.min(i + combineDegree, numChildLevelIterators)
        );
        final Pair<T, Future> iteratorAndFuture = executorSubmitter.apply(subChildren);

        childIteratorsOfNextLevel.add(iteratorAndFuture.lhs);
        combineFutures.add(iteratorAndFuture.rhs);
      } else {
        // If there remains one child, it can be directly connected to a node of the parent level.
        childIteratorsOfNextLevel.add(children.get(i));
      }
    }

    if (childIteratorsOfNextLevel.size() == 1) {
      // This is the root
      return Pair.of(childIteratorsOfNextLevel, combineFutures);
    } else {
      // Build the parent level iterators
      final Pair<List<T>, List<Future>> parentIteratorsAndFutures =
          buildCombineTree(
              childIteratorsOfNextLevel,
              leafCombineDegree,
              intermediateCombineDegree,
              executorSubmitter,
              false
          );
      combineFutures.addAll(parentIteratorsAndFutures.rhs);
      return Pair.of(parentIteratorsAndFutures.lhs, combineFutures);
    }
  }

  private ParallelCombines() {}
}
