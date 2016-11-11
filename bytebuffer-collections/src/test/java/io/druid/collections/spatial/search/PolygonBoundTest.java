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

package io.druid.collections.spatial.search;

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
}
