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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class KllDoublesSketchToStringPostAggregatorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final PostAggregator there = new KllDoublesSketchToStringPostAggregator(
        "post",
        new FieldAccessPostAggregator("field1", "sketch")
    );
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    KllDoublesSketchToStringPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        KllDoublesSketchToStringPostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
  }

  @Test
  public void testToString()
  {
    final PostAggregator postAgg = new KllDoublesSketchToStringPostAggregator(
        "post",
        new FieldAccessPostAggregator("field1", "sketch")
    );

    Assert.assertEquals(
        "KllDoublesSketchToStringPostAggregator{name='post', field=FieldAccessPostAggregator{name='field1', fieldName='sketch'}}",
        postAgg.toString()
    );
  }

  @Test
  public void testComparator()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Comparing sketch summaries is not supported");
    final PostAggregator postAgg = new KllDoublesSketchToStringPostAggregator(
        "post",
        new FieldAccessPostAggregator("field1", "sketch")
    );
    postAgg.getComparator();
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(KllDoublesSketchToStringPostAggregator.class)
                  .withNonnullFields("name", "field")
                  .usingGetClass()
                  .verify();
  }
}
