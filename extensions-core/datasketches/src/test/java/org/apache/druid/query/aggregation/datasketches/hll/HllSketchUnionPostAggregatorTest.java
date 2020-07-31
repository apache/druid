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

package org.apache.druid.query.aggregation.datasketches.hll;

import com.fasterxml.jackson.core.JsonProcessingException;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class HllSketchUnionPostAggregatorTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    final PostAggregator there = new HllSketchUnionPostAggregator(
        "post",
        Arrays.asList(
            new FieldAccessPostAggregator("field1", "sketch"),
            new FieldAccessPostAggregator("field2", "sketch")
        ),
        1024,
        TgtHllType.HLL_8.name()
    );
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    HllSketchUnionPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        HllSketchUnionPostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
  }

  @Test
  public void testToString()
  {
    final PostAggregator postAgg = new HllSketchUnionPostAggregator(
        "post",
        Arrays.asList(
            new FieldAccessPostAggregator("field1", "sketch"),
            new FieldAccessPostAggregator("field2", "sketch")
        ),
        null,
        null
    );

    Assert.assertEquals(
        "HllSketchUnionPostAggregator{name='post', fields=[FieldAccessPostAggregator{name='field1', fieldName='sketch'}, FieldAccessPostAggregator{name='field2', fieldName='sketch'}], lgK=12, tgtHllType=HLL_4}",
        postAgg.toString()
    );
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(HllSketchUnionPostAggregator.class)
                  .withNonnullFields("name", "fields")
                  .usingGetClass()
                  .verify();
  }
}
