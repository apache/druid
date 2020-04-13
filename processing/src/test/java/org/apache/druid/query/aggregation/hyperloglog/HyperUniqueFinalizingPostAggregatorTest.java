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

package org.apache.druid.query.aggregation.hyperloglog;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Random;

/**
 */
public class HyperUniqueFinalizingPostAggregatorTest
{
  private final HashFunction fn = Hashing.murmur3_128();

  @Test
  public void testCompute()
  {
    Random random = new Random(0L);
    HyperUniqueFinalizingPostAggregator postAggregator = new HyperUniqueFinalizingPostAggregator(
        "uniques", "uniques"
    );
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

    for (int i = 0; i < 100; ++i) {
      byte[] hashedVal = fn.hashLong(random.nextLong()).asBytes();
      collector.add(hashedVal);
    }

    double cardinality = (Double) postAggregator.compute(ImmutableMap.of("uniques", collector));

    Assert.assertTrue(cardinality == 99.37233005831612);
  }

  @Test
  public void testComputeRounded()
  {
    Random random = new Random(0L);
    HyperUniqueFinalizingPostAggregator postAggregator = new HyperUniqueFinalizingPostAggregator(
        "uniques", "uniques"
    ).decorate(
        ImmutableMap.of(
            "uniques",
            new CardinalityAggregatorFactory(
                "uniques",
                null,
                Collections.singletonList(DefaultDimensionSpec.of("dummy")),
                false,
                true
            )
        )
    );
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

    for (int i = 0; i < 100; ++i) {
      byte[] hashedVal = fn.hashLong(random.nextLong()).asBytes();
      collector.add(hashedVal);
    }

    Object cardinality = postAggregator.compute(ImmutableMap.of("uniques", collector));

    Assert.assertThat(cardinality, CoreMatchers.instanceOf(Long.class));
    Assert.assertEquals(99L, cardinality);
  }
}
