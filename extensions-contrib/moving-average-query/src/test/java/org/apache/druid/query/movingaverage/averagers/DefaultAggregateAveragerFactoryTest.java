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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;

public class DefaultAggregateAveragerFactoryTest
{

  @Test
  public void testCreateAverager()
  {
    AveragerFactory<?, ?> fac = new DefaultAggregateAveragerFactory("test", 5, 1, "field");
    Assert.assertThat(
        fac.createAverager(ImmutableMap.of("field", new DoubleMaxAggregatorFactory("name", "field"))),
        IsInstanceOf.instanceOf(DefaultAggregateAverager.class)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNotFoundDependentField()
  {
    AveragerFactory<?, ?> fac = new DefaultAggregateAveragerFactory("test", 5, 1, "field");
    Assert.assertThat(
        fac.createAverager(ImmutableMap.of("invalidField", new DoubleMaxAggregatorFactory("name", "invalidField"))),
        IsInstanceOf.instanceOf(DefaultAggregateAverager.class)
    );
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUnsuppportedCreateAverager()
  {
    AveragerFactory<?, ?> fac = new DefaultAggregateAveragerFactory("test", 5, 1, "field");
    Assert.assertThat(
        fac.createAverager(),
        IsInstanceOf.instanceOf(DefaultAggregateAverager.class)
    );
  }

}
