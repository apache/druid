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

package org.apache.druid.query.movingaverage.averagers;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.druid.query.aggregation.datasketches.theta.SketchHolder;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class SketchUnionAveragerTest
{

  @Test
  public void testComputeResult() throws Exception
  {
    BaseAverager<SketchHolder, SketchHolder> avg =
        new SketchUnionAverager(2, "test", "field", 1, Util.DEFAULT_NOMINAL_ENTRIES);

    UpdateSketch sketch1 = UpdateSketch.builder().build();
    for (int key = 0; key < 16; key++) {
      sketch1.update(key);
    }

    UpdateSketch sketch2 = UpdateSketch.builder().build();
    for (int key = 16; key < 32; key++) {
      sketch2.update(key);
    }

    UpdateSketch sketch3 = UpdateSketch.builder().build();
    for (int key = 32; key < 48; key++) {
      sketch2.update(key);
    }

    SketchHolder holder1 = SketchHolder.of(sketch1);
    SketchHolder holder2 = SketchHolder.of(sketch2);
    SketchHolder holder3 = SketchHolder.of(sketch3);

    Union union1 = (Union) SetOperation.builder().build(Family.UNION);
    union1.update(sketch1);
    union1.update(sketch2);

    avg.addElement(Collections.singletonMap("field", holder1), new HashMap<>());
    avg.addElement(Collections.singletonMap("field", holder2), new HashMap<>());

    assertEquals(avg.computeResult().getEstimate(), union1.getResult().getEstimate(), 0);

    avg.addElement(Collections.singletonMap("field", holder3), new HashMap<>());

    Union union2 = (Union) SetOperation.builder().build(Family.UNION);
    union2.update(sketch2);
    union2.update(sketch3);

    assertEquals(avg.computeResult().getEstimate(), union2.getResult().getEstimate(), 0);

  }

}
