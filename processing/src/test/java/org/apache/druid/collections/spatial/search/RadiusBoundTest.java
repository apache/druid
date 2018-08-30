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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class RadiusBoundTest
{
  @Test
  public void testCacheKey()
  {
    final float[] coords0 = new float[]{1.0F, 2.0F};
    final float[] coords1 = new float[]{1.1F, 2.1F};
    Assert.assertArrayEquals(
        new RadiusBound(coords0, 3.0F, 10).getCacheKey(),
        new RadiusBound(coords0, 3.0F, 10).getCacheKey()
    );
    Assert.assertFalse(Arrays.equals(
        new RadiusBound(coords0, 3.0F, 10).getCacheKey(),
        new RadiusBound(coords1, 3.0F, 10).getCacheKey()
    ));
    Assert.assertFalse(Arrays.equals(
        new RadiusBound(coords0, 3.0F, 10).getCacheKey(),
        new RadiusBound(coords0, 3.1F, 10).getCacheKey()
    ));
    Assert.assertFalse(Arrays.equals(
        new RadiusBound(coords0, 3.0F, 10).getCacheKey(),
        new RadiusBound(coords0, 3.0F, 9).getCacheKey()
    ));
  }
}
