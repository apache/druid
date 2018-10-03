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

package org.apache.druid.collections.spatial;

import junit.framework.Assert;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ConciseBitmapFactory;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.collections.spatial.split.LinearGutmanSplitStrategy;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 */
public class RTreeTest
{
  private RTree tree;
  private RTree roaringtree;

  @Before
  public void setUp()
  {
    BitmapFactory bf = new ConciseBitmapFactory();
    tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    BitmapFactory rbf = new RoaringBitmapFactory();
    roaringtree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, rbf), rbf);

  }

  @Test
  public void testInsertNoSplit()
  {
    float[] elem = new float[]{5, 5};
    tree.insert(elem, 1);
    Assert.assertTrue(Arrays.equals(elem, tree.getRoot().getMinCoordinates()));
    Assert.assertTrue(Arrays.equals(elem, tree.getRoot().getMaxCoordinates()));

    tree.insert(new float[]{6, 7}, 2);
    tree.insert(new float[]{1, 3}, 3);
    tree.insert(new float[]{10, 4}, 4);
    tree.insert(new float[]{8, 2}, 5);

    Assert.assertEquals(tree.getRoot().getChildren().size(), 5);

    float[] expectedMin = new float[]{1, 2};
    float[] expectedMax = new float[]{10, 7};

    Assert.assertTrue(Arrays.equals(expectedMin, tree.getRoot().getMinCoordinates()));
    Assert.assertTrue(Arrays.equals(expectedMax, tree.getRoot().getMaxCoordinates()));
    Assert.assertEquals(tree.getRoot().getArea(), 45.0d);
  }

  @Test
  public void testInsertDuplicatesNoSplit()
  {
    tree.insert(new float[]{1, 1}, 1);
    tree.insert(new float[]{1, 1}, 1);
    tree.insert(new float[]{1, 1}, 1);

    Assert.assertEquals(tree.getRoot().getChildren().size(), 3);
  }

  @Test
  public void testInsertDuplicatesNoSplitRoaring()
  {
    roaringtree.insert(new float[]{1, 1}, 1);
    roaringtree.insert(new float[]{1, 1}, 1);
    roaringtree.insert(new float[]{1, 1}, 1);

    Assert.assertEquals(roaringtree.getRoot().getChildren().size(), 3);
  }


  @Test
  public void testSplitOccurs()
  {
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < 100; i++) {
      tree.insert(new float[]{rand.nextFloat(), rand.nextFloat()}, i);
    }

    Assert.assertTrue(tree.getRoot().getChildren().size() > 1);
  }

  @Test
  public void testSplitOccursRoaring()
  {
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < 100; i++) {
      roaringtree.insert(new float[]{rand.nextFloat(), rand.nextFloat()}, i);
    }

    Assert.assertTrue(roaringtree.getRoot().getChildren().size() > 1);
  }

}
