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

import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.spatial.Node;
import io.druid.collections.spatial.RTreeUtils;

import java.util.List;

/**
 */
public class QuadraticGutmanSplitStrategy extends GutmanSplitStrategy
{
  public QuadraticGutmanSplitStrategy(int minNumChildren, int maxNumChildren, BitmapFactory bf)
  {
    super(minNumChildren, maxNumChildren, bf);
  }

  @Override
  public Node[] pickSeeds(List<Node> nodes)
  {
    double highestCost = Double.NEGATIVE_INFINITY;
    int[] highestCostIndices = new int[2];

    for (int i = 0; i < nodes.size() - 1; i++) {
      for (int j = i + 1; j < nodes.size(); j++) {
        double cost = RTreeUtils.getEnclosingArea(nodes.get(i), nodes.get(j)) -
                      nodes.get(i).getArea() - nodes.get(j).getArea();
        if (cost > highestCost) {
          highestCost = cost;
          highestCostIndices[0] = i;
          highestCostIndices[1] = j;
        }
      }
    }

    return new Node[]{nodes.remove(highestCostIndices[0]), nodes.remove(highestCostIndices[1] - 1)};
  }

  @Override
  public Node pickNext(List<Node> nodes, Node[] groups)
  {
    double highestCost = Double.NEGATIVE_INFINITY;
    Node costlyNode = null;
    int counter = 0;
    int index = -1;
    for (Node node : nodes) {
      double group0Cost = RTreeUtils.getEnclosingArea(node, groups[0]);
      double group1Cost = RTreeUtils.getEnclosingArea(node, groups[1]);
      double cost = Math.abs(group0Cost - group1Cost);
      if (cost > highestCost) {
        highestCost = cost;
        costlyNode = node;
        index = counter;
      }
      counter++;
    }

    if (costlyNode != null) {
      nodes.remove(index);
    }

    return costlyNode;
  }
}
