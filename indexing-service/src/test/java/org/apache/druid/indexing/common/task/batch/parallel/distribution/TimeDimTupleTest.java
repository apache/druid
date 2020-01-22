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

package org.apache.druid.indexing.common.task.batch.parallel.distribution;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TimeDimTupleTest
{
  private static final long TIMESTAMP = 1000;
  private static final String DIMENSION1 = "a";
  private static final String DIMENSION2 = "m";
  private static final String DIMENSION3 = "z";

  private TimeDimTuple target;

  @Before
  public void setup()
  {
    target = new TimeDimTuple(TIMESTAMP, DIMENSION2);
  }

  @Test
  public void comparesCorrectlyToSmallerTimestamp()
  {
    Assert.assertThat(target.compareTo(new TimeDimTuple(TIMESTAMP - 1, DIMENSION2)), Matchers.greaterThan(0));
  }

  @Test
  public void comparesCorrectlyToSmallerDimension()
  {
    Assert.assertThat(target.compareTo(new TimeDimTuple(TIMESTAMP, DIMENSION1)), Matchers.greaterThan(0));
  }

  @Test
  public void comparesCorrectlyToEqual()
  {
    Assert.assertEquals(0, target.compareTo(new TimeDimTuple(TIMESTAMP, DIMENSION2)));
  }

  @Test
  public void comparesCorrectlyToBiggerTimestamp()
  {
    Assert.assertThat(target.compareTo(new TimeDimTuple(TIMESTAMP + 1, DIMENSION2)), Matchers.lessThan(0));
  }

  @Test
  public void comparesCorrectlyToBiggerDimension()
  {
    Assert.assertThat(target.compareTo(new TimeDimTuple(TIMESTAMP, DIMENSION3)), Matchers.lessThan(0));
  }
}
