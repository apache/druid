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

package org.apache.druid.query.aggregation.momentsketch;

import org.junit.Assert;
import org.junit.Test;

public class MomentSketchWrapperTest
{
  @Test
  public void testDeserialize()
  {
    MomentSketchWrapper mw = new MomentSketchWrapper(10);
    mw.setCompressed(false);
    mw.add(10);
    byte[] bs = mw.toByteArray();
    MomentSketchWrapper mw2 = MomentSketchWrapper.fromByteArray(bs);

    Assert.assertEquals(10, mw2.getPowerSums()[1], 1e-10);
  }

  @Test
  public void testSimpleSolve()
  {
    MomentSketchWrapper mw = new MomentSketchWrapper(13);
    mw.setCompressed(true);
    for (int x = 0; x < 101; x++) {
      mw.add((double) x);
    }
    double[] ps = {0.0, 0.5, 1.0};
    double[] qs = mw.getQuantiles(ps);
    Assert.assertEquals(0, qs[0], 1.0);
    Assert.assertEquals(50, qs[1], 1.0);
  }
}
