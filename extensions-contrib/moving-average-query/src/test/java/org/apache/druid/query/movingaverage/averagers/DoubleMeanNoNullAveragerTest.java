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

public class DoubleMeanNoNullAveragerTest
{
  @Test
  public void testComputeResult()
  {
    BaseAverager<Number, Double> avg = new DoubleMeanNoNullAverager(3, "test", "field", 1);

    Assert.assertEquals(Double.NaN, avg.computeResult(), 0.0);

    avg.addElement(Collections.singletonMap("field", 3.0), new HashMap<>());
    Assert.assertEquals(3.0, avg.computeResult(), 0.0);

    avg.addElement(Collections.singletonMap("field", 3.0), new HashMap<>());
    Assert.assertEquals(3.0, avg.computeResult(), 0.0);

    avg.addElement(Collections.singletonMap("field", 0), new HashMap<>());
    Assert.assertEquals(2.0, avg.computeResult(), 0.0);

    avg.addElement(Collections.singletonMap("field", 2.0), new HashMap<>());
    avg.addElement(Collections.singletonMap("field", 2.0), new HashMap<>());
    avg.addElement(Collections.singletonMap("field", 2.0), new HashMap<>());
    Assert.assertEquals(2.0, avg.computeResult(), 0.0);

    avg.skip();
    Assert.assertEquals(2.0, avg.computeResult(), 0.0);

    // testing cycleSize functionality
    BaseAverager<Number, Double> averager = new DoubleMeanNoNullAverager(14, "test", "field", 7);

    averager.addElement(Collections.singletonMap("field", 2.0), new HashMap<>());
    Assert.assertEquals(2.0, averager.computeResult(), 0.0);

    averager.addElement(Collections.singletonMap("field", 4.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 5.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 6.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 7.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 8.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 9.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", null), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 11.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 12.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 13.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 14.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 15.0), new HashMap<>());
    averager.addElement(Collections.singletonMap("field", 16.0), new HashMap<>());

    Assert.assertEquals(7.5, averager.computeResult(), 0.0);

    averager.addElement(Collections.singletonMap("field", 3.0), new HashMap<>());
    Assert.assertEquals(8.5, averager.computeResult(), 0.0);
  }
}
