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

public class DoubleSumAveragerTest
{

  @Test
  public void testComputeResult() throws Exception
  {
    BaseAverager<Number, Double> avg = new DoubleSumAverager(3, "test", "field", 1);

    Assert.assertEquals(0.0, avg.computeResult(), 0.0);

    avg.addElement(Collections.singletonMap("field", 3.0), new HashMap<>());
    Assert.assertEquals(3.0, avg.computeResult(), 0.0);

    avg.addElement(Collections.singletonMap("field", 3.0), new HashMap<>());
    Assert.assertEquals(6.0, avg.computeResult(), 0.0);

    avg.addElement(Collections.singletonMap("field", new Integer(0)), new HashMap<>());
    Assert.assertEquals(6.0, avg.computeResult(), 0.0);

    avg.addElement(Collections.singletonMap("field", 2.5), new HashMap<>());
    avg.addElement(Collections.singletonMap("field", 2.0), new HashMap<>());
    avg.addElement(Collections.singletonMap("field", 2.0), new HashMap<>());
    Assert.assertEquals(6.5, avg.computeResult(), 0.0);

    avg.skip();
    Assert.assertEquals(4.0, avg.computeResult(), 0.0);

  }

}
