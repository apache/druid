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

package org.apache.druid.query.aggregation.histogram;

import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class BucketsPostAggregatorTest
{
  @Test
  public void testSerde() throws Exception
  {
    BucketsPostAggregator aggregator1 =
        new BucketsPostAggregator("buckets_post_aggregator", "test_field", 2f, 4f);

    DefaultObjectMapper mapper = new DefaultObjectMapper();
    BucketsPostAggregator aggregator2 = mapper.readValue(
        mapper.writeValueAsString(aggregator1),
        BucketsPostAggregator.class
    );

    Assert.assertEquals(aggregator1.getBucketSize(), aggregator2.getBucketSize(), 0.0001);
    Assert.assertEquals(aggregator1.getOffset(), aggregator2.getOffset(), 0.0001);
    Assert.assertArrayEquals(aggregator1.getCacheKey(), aggregator2.getCacheKey());
    Assert.assertEquals(aggregator1.getDependentFields(), aggregator2.getDependentFields());
  }
}
