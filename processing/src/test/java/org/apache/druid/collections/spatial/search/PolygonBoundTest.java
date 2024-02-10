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

public class PolygonBoundTest
{
  @Test
  public void testCacheKey()
  {
    Assert.assertArrayEquals(
        PolygonBound.from(new double[]{1F, 2F, 3F}, new double[]{0F, 2F, 0F}, 1).getCacheKey(),
        PolygonBound.from(new double[]{1F, 2F, 3F}, new double[]{0F, 2F, 0F}, 1).getCacheKey()
    );
    Assert.assertFalse(Arrays.equals(
        PolygonBound.from(new double[]{1F, 2F, 3F}, new double[]{0F, 2F, 0F}, 1).getCacheKey(),
        PolygonBound.from(new double[]{1F, 2F, 3F}, new double[]{0F, 2F, 1F}, 1).getCacheKey()
    ));
    Assert.assertFalse(Arrays.equals(
        PolygonBound.from(new double[]{1F, 2F, 3F}, new double[]{0F, 2F, 0F}, 1).getCacheKey(),
        PolygonBound.from(new double[]{1F, 2F, 2F}, new double[]{0F, 2F, 0F}, 1).getCacheKey()
    ));
    Assert.assertFalse(Arrays.equals(
        PolygonBound.from(new double[]{1F, 2F, 3F}, new double[]{0F, 2F, 0F}, 1).getCacheKey(),
        PolygonBound.from(new double[]{1F, 2F, 3F}, new double[]{0F, 2F, 0F}, 2).getCacheKey()
    ));
  }

  @Test
  public void testContains()
  {
    final PolygonBound triangle = PolygonBound.from(new double[]{1f, 4f, 7f}, new double[]{1f, 4f, 1f});
    final float delta = 1e-5f;

    Assert.assertTrue(triangle.contains(new double[]{1f, 1f}));
    Assert.assertFalse(triangle.contains(new double[]{1f, 1f - delta}));
    Assert.assertFalse(triangle.contains(new double[]{1f, 1f + delta}));
    Assert.assertTrue(triangle.contains(new double[]{1f + delta, 1f}));
    Assert.assertFalse(triangle.contains(new double[]{1f - delta, 1f}));
    Assert.assertTrue(triangle.contains(new double[]{1f + delta, 1f}));
    Assert.assertFalse(triangle.contains(new double[]{1f - delta, 1f}));
    Assert.assertTrue(triangle.contains(new double[]{5f, 1f}));
    Assert.assertFalse(triangle.contains(new double[]{1f, 5f}));
    Assert.assertTrue(triangle.contains(new double[]{3f, 2f}));

    final PolygonBound rightTriangle = PolygonBound.from(new double[]{1f, 1f, 5f}, new double[]{1f, 5f, 1f});

    Assert.assertTrue(rightTriangle.contains(new double[]{1f, 5f}));
    Assert.assertTrue(rightTriangle.contains(new double[]{2f, 4f}));
    Assert.assertTrue(rightTriangle.contains(new double[]{2f - delta, 4f}));
    Assert.assertFalse(rightTriangle.contains(new double[]{2f + delta, 4f}));
    Assert.assertTrue(rightTriangle.contains(new double[]{2f, 4f - delta}));
    Assert.assertFalse(rightTriangle.contains(new double[]{2f, 4f + delta}));
    Assert.assertTrue(rightTriangle.contains(new double[]{3f - delta, 3f}));
    Assert.assertFalse(rightTriangle.contains(new double[]{3f + delta, 3f}));
    Assert.assertTrue(rightTriangle.contains(new double[]{3f, 3f - delta}));
    Assert.assertFalse(rightTriangle.contains(new double[]{3f, 3f + delta}));
  }

  @Test
  public void testHighPrecisions()
  {
    //37.82460331205531, -122.50851323395436 Black Sand Beach
    //37.79378584960722, -122.48344917652936 Bakers Beach
    //37.82872192254861, -122.48597242173493 Golden Gate view point

    final PolygonBound triangle = PolygonBound.from(
        new double[]{37.82460331205531, 37.79378584960722, 37.82872192254861},
        new double[]{-122.50851323395436, -122.48344917652936, -122.48597242173493}
    );
    // points near triangle edges
    Assert.assertTrue(triangle.contains(new double[]{37.82668550138975, -122.48783179067323}));
    Assert.assertTrue(triangle.contains(new double[]{37.813408325545275, -122.48605838780342}));
    Assert.assertFalse(triangle.contains(new double[]{37.80812634358083, -122.49676991156807}));
    Assert.assertFalse(triangle.contains(new double[]{37.81832968852414, -122.4843583756818}));
  }

  @Test
  public void testEqualsContract()
  {
    EqualsVerifier.forClass(PolygonBound.class)
                  .usingGetClass()
                  .verify();
  }
}
