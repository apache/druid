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

package org.apache.druid.collections.spatial.split;

import junit.framework.Assert;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ConciseBitmapFactory;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.collections.spatial.Node;
import org.apache.druid.collections.spatial.Point;
import org.apache.druid.collections.spatial.RTree;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 */
public class LinearGutmanSplitStrategyTest
{
  @Test
  public void testPickSeeds()
  {
    BitmapFactory bf = new ConciseBitmapFactory();
    LinearGutmanSplitStrategy strategy = new LinearGutmanSplitStrategy(0, 50, bf);
    Node node = new Node(new float[2], new float[2], true, bf);

    node.addChild(new Point(new float[]{3, 7}, 1, bf));
    node.addChild(new Point(new float[]{1, 6}, 1, bf));
    node.addChild(new Point(new float[]{9, 8}, 1, bf));
    node.addChild(new Point(new float[]{2, 5}, 1, bf));
    node.addChild(new Point(new float[]{4, 4}, 1, bf));
    node.enclose();

    Node[] groups = strategy.split(node);
    Assert.assertEquals(groups[0].getMinCoordinates()[0], 1.0f);
    Assert.assertEquals(groups[0].getMinCoordinates()[1], 4.0f);
    Assert.assertEquals(groups[1].getMinCoordinates()[0], 9.0f);
    Assert.assertEquals(groups[1].getMinCoordinates()[1], 8.0f);
  }

  @Test
  public void testPickSeedsRoaring()
  {
    BitmapFactory bf = new RoaringBitmapFactory();
    LinearGutmanSplitStrategy strategy = new LinearGutmanSplitStrategy(0, 50, bf);
    Node node = new Node(new float[2], new float[2], true, bf);

    node.addChild(new Point(new float[]{3, 7}, 1, bf));
    node.addChild(new Point(new float[]{1, 6}, 1, bf));
    node.addChild(new Point(new float[]{9, 8}, 1, bf));
    node.addChild(new Point(new float[]{2, 5}, 1, bf));
    node.addChild(new Point(new float[]{4, 4}, 1, bf));
    node.enclose();

    Node[] groups = strategy.split(node);
    Assert.assertEquals(groups[0].getMinCoordinates()[0], 1.0f);
    Assert.assertEquals(groups[0].getMinCoordinates()[1], 4.0f);
    Assert.assertEquals(groups[1].getMinCoordinates()[0], 9.0f);
    Assert.assertEquals(groups[1].getMinCoordinates()[1], 8.0f);
  }


  @Test
  public void testNumChildrenSize()
  {
    BitmapFactory bf = new ConciseBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < 100; i++) {
      tree.insert(new float[]{rand.nextFloat(), rand.nextFloat()}, i);
    }

    Assert.assertTrue(getNumPoints(tree.getRoot()) >= tree.getSize());
  }

  @Test
  public void testNumChildrenSizeRoaring()
  {
    BitmapFactory bf = new RoaringBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < 100; i++) {
      tree.insert(new float[]{rand.nextFloat(), rand.nextFloat()}, i);
    }

    Assert.assertTrue(getNumPoints(tree.getRoot()) >= tree.getSize());
  }

  private int getNumPoints(Node node)
  {
    int total = 0;
    if (node.isLeaf()) {
      total += node.getChildren().size();
    } else {
      for (Node child : node.getChildren()) {
        total += getNumPoints(child);
      }
    }
    return total;
  }
}
