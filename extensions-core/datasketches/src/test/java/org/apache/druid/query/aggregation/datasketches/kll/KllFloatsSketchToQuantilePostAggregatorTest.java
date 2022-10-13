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

package org.apache.druid.query.aggregation.datasketches.kll;

import com.fasterxml.jackson.core.JsonProcessingException;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.junit.Assert;
import org.junit.Test;

public class KllFloatsSketchToQuantilePostAggregatorTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    final PostAggregator there = new KllFloatsSketchToQuantilePostAggregator(
        "post",
        new FieldAccessPostAggregator("field1", "sketch"),
        0.5
    );
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    KllFloatsSketchToQuantilePostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        KllFloatsSketchToQuantilePostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
  }

  @Test
  public void testToString()
  {
    final PostAggregator postAgg = new KllFloatsSketchToQuantilePostAggregator(
        "post",
        new FieldAccessPostAggregator("field1", "sketch"),
        0.5
    );

    Assert.assertEquals(
        "KllFloatsSketchToQuantilePostAggregator{name='post', field=FieldAccessPostAggregator{name='field1', fieldName='sketch'}, fraction=0.5}",
        postAgg.toString()
    );
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(KllFloatsSketchToQuantilePostAggregator.class)
                  .withNonnullFields("name", "field", "fraction")
                  .usingGetClass()
                  .verify();
  }
}
