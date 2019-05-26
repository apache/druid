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

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class LongMaxAveragerTest
{

  @Test
  public void testComputeResult()
  {
    BaseAverager<Number, Long> avg = new LongMaxAverager(3, "test", "field", 1);

    assertEquals(Long.MIN_VALUE, (long) avg.computeResult());

    avg.addElement(Collections.singletonMap("field", -1000000L), new HashMap<>());
    assertEquals(-1000000, (long) avg.computeResult());

    avg.addElement(Collections.singletonMap("field", 1L), new HashMap<>());
    assertEquals(1, (long) avg.computeResult());

    avg.addElement(Collections.singletonMap("field", new Integer(1)), new HashMap<>());
    assertEquals(1, (long) avg.computeResult());

    avg.addElement(Collections.singletonMap("field", 5L), new HashMap<>());
    avg.addElement(Collections.singletonMap("field", 3L), new HashMap<>());
    avg.addElement(Collections.singletonMap("field", 2L), new HashMap<>());
    assertEquals(5, (long) avg.computeResult());

    avg.skip();
    assertEquals(3, (long) avg.computeResult());
  }

}
