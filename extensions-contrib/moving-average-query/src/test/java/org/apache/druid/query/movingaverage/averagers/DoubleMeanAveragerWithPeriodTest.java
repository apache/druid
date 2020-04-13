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

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

public class DoubleMeanAveragerWithPeriodTest
{
  @Test
  public void testComputeResult()
  {
    BaseAverager<Number, Double> averager = new DoubleMeanAverager(14, "test", "field", 7);

    averager.addElement(Collections.singletonMap("field", 7.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 1.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 2.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 3.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 4.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 5.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 6.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 7.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 1.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 2.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 3.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 4.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 5.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 6.0), new HashMap<>());

    Assert.assertEquals(7, averager.computeResult(), 0.0); // (7+7)/2

    averager.addElement(Collections.singletonMap("field", 3.0), new HashMap<>());
    Assert.assertEquals(1, averager.computeResult(), 0.0); // (1+1)/2

    BaseAverager<Number, Double> averager1 = new DoubleMeanAverager(14, "test", "field", 3);

    averager1.addElement(Collections.singletonMap("field", 1.0), new HashMap<>());
    averager1.addElement(Collections.singletonMap("field", 2.0), new HashMap<>());
    averager1.addElement(Collections.singletonMap("field", 3.0), new HashMap<>());
    averager1.addElement(Collections.singletonMap("field", 1.0), new HashMap<>());
    averager1.addElement(Collections.singletonMap("field", 2.0), new HashMap<>());
    averager1.addElement(Collections.singletonMap("field", 3.0), new HashMap<>());
    averager1.addElement(Collections.singletonMap("field", 1.0), new HashMap<>());
    averager1.addElement(Collections.singletonMap("field", 2.0), new HashMap<>());
    averager1.addElement(Collections.singletonMap("field", 3.0), new HashMap<>());
    averager1.addElement(Collections.singletonMap("field", 1.0), new HashMap<>());
    averager1.addElement(Collections.singletonMap("field", 2.0), new HashMap<>());
    averager1.addElement(Collections.singletonMap("field", 3.0), new HashMap<>());
    averager1.addElement(Collections.singletonMap("field", 1.0), new HashMap<>());
    averager1.addElement(Collections.singletonMap("field", 2.0), new HashMap<>());

    Assert.assertEquals(1, averager1.computeResult(), 0.0); // (1+1+1+1+1)/5

    Assert.assertEquals(2, averager1.computeResult(), 0.0); // (2+2+2+2+2)/5

    Assert.assertEquals(13.0 / 5, averager1.computeResult(), 0.0); // (3+3+3+3+1)/5
  }
}
