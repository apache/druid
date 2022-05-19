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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.PostAggregator;
import org.junit.Assert;
import org.junit.Test;

public class BucketsPostAggregatorTest
{
  @Test
  public void testSerde() throws Exception
  {
    BucketsPostAggregator there =
        new BucketsPostAggregator("buckets_post_aggregator", "test_field", 2f, 4f);

    DefaultObjectMapper mapper = new DefaultObjectMapper();
    BucketsPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        BucketsPostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertEquals(there.getBucketSize(), andBackAgain.getBucketSize(), 0.0001);
    Assert.assertEquals(there.getOffset(), andBackAgain.getOffset(), 0.0001);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
    Assert.assertEquals(there.getDependentFields(), andBackAgain.getDependentFields());
  }

  @Test
  public void testToString()
  {
    PostAggregator postAgg =
        new BucketsPostAggregator("buckets_post_aggregator", "test_field", 2f, 4f);

    Assert.assertEquals(
        "BucketsPostAggregator{name='buckets_post_aggregator', fieldName='test_field', bucketSize=2.0, offset=4.0}",
        postAgg.toString()
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(BucketsPostAggregator.class)
                  .withNonnullFields("name", "fieldName")
                  .usingGetClass()
                  .verify();
  }
}
