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
        PolygonBound.from(new float[]{1F, 2F, 3F}, new float[]{0F, 2F, 0F}, 1).getCacheKey(),
        PolygonBound.from(new float[]{1F, 2F, 3F}, new float[]{0F, 2F, 0F}, 1).getCacheKey()
    );
    Assert.assertFalse(Arrays.equals(
        PolygonBound.from(new float[]{1F, 2F, 3F}, new float[]{0F, 2F, 0F}, 1).getCacheKey(),
        PolygonBound.from(new float[]{1F, 2F, 3F}, new float[]{0F, 2F, 1F}, 1).getCacheKey()
    ));
    Assert.assertFalse(Arrays.equals(
        PolygonBound.from(new float[]{1F, 2F, 3F}, new float[]{0F, 2F, 0F}, 1).getCacheKey(),
        PolygonBound.from(new float[]{1F, 2F, 2F}, new float[]{0F, 2F, 0F}, 1).getCacheKey()
    ));
    Assert.assertFalse(Arrays.equals(
        PolygonBound.from(new float[]{1F, 2F, 3F}, new float[]{0F, 2F, 0F}, 1).getCacheKey(),
        PolygonBound.from(new float[]{1F, 2F, 3F}, new float[]{0F, 2F, 0F}, 2).getCacheKey()
    ));
  }

  @Test
  public void testContains()
  {
    final PolygonBound triangle = PolygonBound.from(new float[]{1f, 4f, 7f}, new float[]{1f, 4f, 1f});
    final float delta = 1e-5f;

    Assert.assertTrue(triangle.contains(new float[]{1f, 1f}));
    Assert.assertFalse(triangle.contains(new float[]{1f, 1f - delta}));
    Assert.assertFalse(triangle.contains(new float[]{1f, 1f + delta}));
    Assert.assertTrue(triangle.contains(new float[]{1f + delta, 1f}));
    Assert.assertFalse(triangle.contains(new float[]{1f - delta, 1f}));
    Assert.assertTrue(triangle.contains(new float[]{1f + delta, 1f}));
    Assert.assertFalse(triangle.contains(new float[]{1f - delta, 1f}));
    Assert.assertTrue(triangle.contains(new float[]{5f, 1f}));
    Assert.assertFalse(triangle.contains(new float[]{1f, 5f}));
    Assert.assertTrue(triangle.contains(new float[]{3f, 2f}));

    final PolygonBound rightTriangle = PolygonBound.from(new float[]{1f, 1f, 5f}, new float[]{1f, 5f, 1f});

    Assert.assertTrue(rightTriangle.contains(new float[]{1f, 5f}));
    Assert.assertTrue(rightTriangle.contains(new float[]{2f, 4f}));
    Assert.assertTrue(rightTriangle.contains(new float[]{2f - delta, 4f}));
    Assert.assertFalse(rightTriangle.contains(new float[]{2f + delta, 4f}));
    Assert.assertTrue(rightTriangle.contains(new float[]{2f, 4f - delta}));
    Assert.assertFalse(rightTriangle.contains(new float[]{2f, 4f + delta}));
    Assert.assertTrue(rightTriangle.contains(new float[]{3f - delta, 3f}));
    Assert.assertFalse(rightTriangle.contains(new float[]{3f + delta, 3f}));
    Assert.assertTrue(rightTriangle.contains(new float[]{3f, 3f - delta}));
    Assert.assertFalse(rightTriangle.contains(new float[]{3f, 3f + delta}));
  }

  @Test
  public void testHighPrecisions()
  {
    //37.82460331205531, -122.50851323395436 Black Sand Beach
    //37.79378584960722, -122.48344917652936 Bakers Beach
    //37.82872192254861, -122.48597242173493 Golden Gate view point

    final PolygonBound triangle = PolygonBound.from(
        new float[]{37.82460331205531f, 37.79378584960722f, 37.82872192254861f},
        new float[]{-122.50851323395436f, -122.48344917652936f, -122.48597242173493f}
    );
    // points near triangle edges
    Assert.assertTrue(triangle.contains(new float[]{37.82668550138975f, -122.48783179067323f}));
    Assert.assertTrue(triangle.contains(new float[]{37.813408325545275f, -122.48605838780342f}));
    Assert.assertFalse(triangle.contains(new float[]{37.80812634358083f, -122.49676991156807f}));
    Assert.assertFalse(triangle.contains(new float[]{37.81832968852414f, -122.4843583756818f}));
  }

  @Test
  public void testEqualsContract()
  {
    EqualsVerifier.forClass(PolygonBound.class)
                  .usingGetClass()
                  .verify();
  }
}
