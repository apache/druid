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

package org.apache.druid.frame.processor;

import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.ints.IntComparator;
import org.apache.druid.java.util.common.IAE;

import java.util.Arrays;

/**
 * Tree-of-losers tournament tree used for K-way merging. The tree contains a fixed set of elements, from 0 (inclusive)
 * to {@link #numElements} (exclusive).
 *
 * The tree represents a tournament played amongst the elements. At all times each node of the tree contains the loser
 * of the match at that node. The winners of the matches are not explicitly stored, except for the overall winner of
 * the tournament, which is stored in {@code tree[0]}.
 *
 * When used as part of k-way merge, expected usage is call {@link #getMin()} to retrieve a run number, then read
 * an element from the run. On the next call to {@link #getMin()}, the tree internally calls {@link #update()} to
 * handle the case where the min needs to change.
 *
 * Refer to https://en.wikipedia.org/wiki/K-way_merge_algorithm#Tournament_Tree for additional details.
 */
public class TournamentTree
{
  /**
   * Complete binary tree, with the overall winner (least element) in slot 0, the root of the loser tree in slot 1, and
   * otherwise the node in slot i has children in slots 2*i and (2*i)+1. The final layer of the tree, containing the
   * actual elements [0..numElements), is not stored in this array (it is implicit).
   */
  private final int[] tree;

  /**
   * Number of elements in the tree.
   */
  private final int numElements;

  /**
   * Number of elements, rounded up to the nearest power of two.
   */
  private final int numElementsRounded;

  /**
   * Comparator for the elements of the tree.
   */
  private final IntComparator comparator;

  /**
   * Whether this tree has been initialized.
   */
  private boolean initialized;

  /**
   * Creates a tree with a certain number of elements.
   *
   * @param numElements number of elements in the tree
   * @param comparator  comparator for the elements. Smaller elements "win".
   */
  public TournamentTree(final int numElements, final IntComparator comparator)
  {
    if (numElements < 1) {
      throw new IAE("Must have at least one element");
    }

    this.numElements = numElements;
    this.numElementsRounded = HashCommon.nextPowerOfTwo(numElements);
    this.comparator = comparator;
    this.tree = new int[numElementsRounded];
  }

  /**
   * Get the current minimum element (the overall winner, i.e., the run to pull the next element from in the
   * K-way merge).
   */
  public int getMin()
  {
    if (!initialized) {
      // Defer initialization until the first getMin() call, since the tree object might be created before the
      // comparator is fully valid. (The comparator is typically not valid until at least one row is available
      // from each run.)
      initialize();
      initialized = true;
    }
    update();
    return tree[0];
  }

  @Override
  public String toString()
  {
    return "TournamentTree{" +
           "numElements=" + numElementsRounded +
           ", tree=" + Arrays.toString(tree) +
           '}';
  }

  /**
   * Returns the backing array of the tree. Used in tests.
   */
  @VisibleForTesting
  int[] backingArray()
  {
    return tree;
  }

  /**
   * Initializes the tree by running a full tournament. At the conclusion of this method, all nodes of {@link #tree}
   * are filled in with the loser for the "game" played at that node, except for {@code tree[0]}, which contains the
   * overall winner (least element).
   */
  private void initialize()
  {
    if (numElements == 1) {
      return;
    }

    // Allocate a winner tree, which stores the winner in each node (rather than loser). We'll use this temporarily in
    // this method, but it won't be stored long-term.
    final int[] winnerTree = new int[numElementsRounded];

    // Populate the lowest layer of the loser and winner trees. For example: with elements 0, 1, 2, 3, we'll
    // compare 0 vs 1 and 2 vs 3.
    for (int i = 0; i < numElementsRounded; i += 2) {
      final int cmp = compare(i, i + 1);
      final int loser, winner;
      if (cmp <= 0) {
        winner = i;
        loser = i + 1;
      } else {
        winner = i + 1;
        loser = i;
      }

      final int nodeIndex = (tree.length + i) >> 1;
      tree[nodeIndex] = loser;
      winnerTree[nodeIndex] = winner;
    }

    // Populate all other layers of the loser and winner trees.
    for (int layerSize = numElementsRounded >> 1; layerSize > 1; layerSize >>= 1) {
      for (int i = 0; i < layerSize; i += 2) {
        // Size of a layer is also the starting offset of the layer, so node i of this layer is at layerSize + i.
        final int left = winnerTree[layerSize + i];
        final int right = winnerTree[layerSize + i + 1];
        final int cmp = compare(left, right);
        final int loser, winner;
        if (cmp <= 0) {
          winner = left;
          loser = right;
        } else {
          winner = right;
          loser = left;
        }

        final int nodeIndex = (layerSize + i) >> 1;
        tree[nodeIndex] = loser;
        winnerTree[nodeIndex] = winner;
      }
    }

    // Populate tree[0], overall winner; discard winnerTree.
    tree[0] = winnerTree[1];
  }

  /**
   * Re-play the tournament from leaf to root, assuming the winner (stored in {@code tree[0]} may have changed its
   * ordering relative to other elements.
   */
  private void update()
  {
    int current = tree[0];
    for (int nodeIndex = ((current & ~1) + tree.length) >> 1; nodeIndex >= 1; nodeIndex >>= 1) {
      int nodeLoser = tree[nodeIndex];
      final int cmp = compare(current, nodeLoser);
      if (cmp > 0) {
        tree[nodeIndex] = current;
        current = nodeLoser;
      }
    }
    tree[0] = current;
  }

  /**
   * Compare two elements, which may be outside {@link #numElements}.
   */
  private int compare(int a, int b)
  {
    if (b >= numElements || a >= numElements) {
      return Integer.compare(a, b);
    } else {
      return comparator.compare(a, b);
    }
  }
}
