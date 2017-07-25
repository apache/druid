/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.collections.spatial.split;

import com.google.common.collect.Lists;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.spatial.Node;
import io.druid.collections.spatial.RTreeUtils;

import java.util.Arrays;
import java.util.List;

/**
 */
public abstract class GutmanSplitStrategy implements SplitStrategy
{
  private final int minNumChildren;
  private final int maxNumChildren;
  private final BitmapFactory bf;

  protected GutmanSplitStrategy(int minNumChildren, int maxNumChildren, BitmapFactory b)
  {
    this.minNumChildren = minNumChildren;
    this.maxNumChildren = maxNumChildren;
    this.bf = b;
  }

  @Override
  public boolean needToSplit(Node node)
  {
    return (node.getChildren().size() > maxNumChildren);
  }

  /**
   * This algorithm is from the original paper.
   *
   * Algorithm Split. Divide a set of M+1 index entries into two groups.
   *
   * S1. [Pick first entry for each group]. Apply Algorithm {@link #pickSeeds(List)} to choose
   * two entries to be the first elements of the groups. Assign each to a group.
   *
   * S2. [Check if done]. If all entries have been assigned, stop. If one group has so few entries that all the rest
   * must be assigned to it in order for it to have the minimum number m, assign them and stop.
   *
   * S3. [Select entry to assign]. Invoke Algorithm {@link #pickNext(List, Node[])}
   * to choose the next entry to assign. Add it to the group whose covering rectangle will have to be enlarged least to
   * accommodate it. Resolve ties by adding the entry to the group smaller area, then to the one with fewer entries, then
   * to either. Repeat from S2.
   */
  @Override
  public Node[] split(Node node)
  {
    List<Node> children = Lists.newArrayList(node.getChildren());
    Node[] seeds = pickSeeds(children);

    node.clear();
    node.addChild(seeds[0]);
    node.addToBitmapIndex(seeds[0]);

    Node group1 = new Node(
        Arrays.copyOf(seeds[1].getMinCoordinates(), seeds[1].getMinCoordinates().length),
        Arrays.copyOf(seeds[1].getMaxCoordinates(), seeds[1].getMaxCoordinates().length),
        Lists.newArrayList(seeds[1]),
        node.isLeaf(),
        node.getParent(),
        bf.makeEmptyMutableBitmap()
    );
    group1.addToBitmapIndex(seeds[1]);
    if (node.getParent() != null) {
      node.getParent().addChild(group1);
    }
    Node[] groups = new Node[]{
        node, group1
    };

    RTreeUtils.enclose(groups);

    while (!children.isEmpty()) {
      for (Node group : groups) {
        if (group.getChildren().size() + children.size() <= minNumChildren) {
          for (Node child : children) {
            group.addToBitmapIndex(child);
            group.addChild(child);
          }
          RTreeUtils.enclose(groups);
          return groups;
        }
      }

      Node nextToAssign = pickNext(children, groups);
      double group0ExpandedArea = RTreeUtils.getEnclosingArea(groups[0], nextToAssign);
      double group1ExpandedArea = RTreeUtils.getEnclosingArea(groups[1], nextToAssign);

      Node optimal;
      if (group0ExpandedArea < group1ExpandedArea) {
        optimal = groups[0];
      } else if (group0ExpandedArea == group1ExpandedArea) {
        if (groups[0].getArea() < groups[1].getArea()) {
          optimal = groups[0];
        } else {
          optimal = groups[1];
        }
      } else {
        optimal = groups[1];
      }

      optimal.addToBitmapIndex(nextToAssign);
      optimal.addChild(nextToAssign);
      optimal.enclose();
    }

    return groups;
  }

  public abstract Node[] pickSeeds(List<Node> nodes);

  public abstract Node pickNext(List<Node> nodes, Node[] groups);
}
