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
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

public class BaseAveragerFactoryTest
{
  private AveragerFactory<Long, Long> fac;

  @Before
  public void setup()
  {
    fac = new BaseAveragerFactory<Long, Long>("test", 5, "field", 1)
    {
      @Override
      public Averager<Long> createAverager()
      {
        return null;
      }

      @Override
      public Comparator<Long> getComparator()
      {
        return null;
      }
    };
  }

  @Test
  public void testGetDependentFields()
  {
    List<String> dependentFields = fac.getDependentFields();
    Assert.assertEquals(1, dependentFields.size());
    Assert.assertEquals("field", dependentFields.get(0));
  }

  @Test
  public void testFinalization()
  {
    Long input = 5L;
    Assert.assertEquals(input, fac.finalizeComputation(input));
  }
}
