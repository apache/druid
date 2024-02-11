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

package org.apache.druid.collections.spatial.search;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class RectangularBoundTest
{
  @Test
  public void testCacheKey()
  {
    Assert.assertArrayEquals(
        new RectangularBound(new double[]{1F, 1F}, new double[]{2F, 2F}, 1).getCacheKey(),
        new RectangularBound(new double[]{1F, 1F}, new double[]{2F, 2F}, 1).getCacheKey()
    );
    Assert.assertFalse(Arrays.equals(
        new RectangularBound(new double[]{1F, 1F}, new double[]{2F, 2F}, 1).getCacheKey(),
        new RectangularBound(new double[]{1F, 1F}, new double[]{2F, 3F}, 1).getCacheKey()
    ));
    Assert.assertFalse(Arrays.equals(
        new RectangularBound(new double[]{1F, 1F}, new double[]{2F, 2F}, 1).getCacheKey(),
        new RectangularBound(new double[]{1F, 0F}, new double[]{2F, 2F}, 1).getCacheKey()
    ));
    Assert.assertFalse(Arrays.equals(
        new RectangularBound(new double[]{1F, 1F}, new double[]{2F, 2F}, 1).getCacheKey(),
        new RectangularBound(new double[]{1F, 1F}, new double[]{2F, 2F}, 2).getCacheKey()
    ));
  }

  @Test
  public void testRectangularBound()
  {
    double[][] insidePoints = new double[][]{
        {37.795717853074635, -122.40906979480418},
        {37.79625791859653, -122.39638788940042},
        {37.79685798676811, -122.39335030726777},
        {37.7966179600844, -122.39798262002006}
    };
    double[][] outsidePoints = new double[][]{
        {37.79805810848854, -122.39236309307468},
        {37.78197485768925, -122.41886599718191},
        {37.798298130492945, -122.39608413118715},
        {37.783595343766216, -122.41932163450181}
    };
    RectangularBound rectangularBound = new RectangularBound(
        new double[]{37.78185482027019, -122.41795472254213},
        new double[]{37.797638168104185, -122.39228715352137},
        10
    );
    for (double[] insidePoint : insidePoints) {
      Assert.assertTrue(rectangularBound.contains(insidePoint));
    }
    for (double[] outsidePoint : outsidePoints) {
      Assert.assertFalse(rectangularBound.contains(outsidePoint));
    }
  }

  @Test
  public void testEqualsContract()
  {
    EqualsVerifier.forClass(RectangularBound.class)
                  .usingGetClass()
                  .verify();
  }
}
