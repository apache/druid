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

package org.apache.druid.query.aggregation.datasketches.tuple;

import com.fasterxml.jackson.core.JsonProcessingException;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ArrayOfDoublesSketchToEstimateAndBoundsPostAggregatorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final PostAggregator there = new ArrayOfDoublesSketchToEstimateAndBoundsPostAggregator(
        "a",
        new ConstantPostAggregator("", 0),
        null
    );
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    ArrayOfDoublesSketchToEstimateAndBoundsPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        ArrayOfDoublesSketchToEstimateAndBoundsPostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
  }

  @Test
  public void testToString()
  {
    PostAggregator postAgg = new ArrayOfDoublesSketchToEstimateAndBoundsPostAggregator(
        "a",
        new ConstantPostAggregator("", 0),
        null
    );

    Assert.assertEquals(
        "ArrayOfDoublesSketchToEstimateAndBoundsPostAggregator{name='a', field=ConstantPostAggregator{name='', constantValue=0}, numStdDevs=1}",
        postAgg.toString()
    );
  }

  @Test
  public void testComparator()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Comparing arrays of estimates and error bounds is not supported");
    final PostAggregator postAgg = new ArrayOfDoublesSketchToEstimateAndBoundsPostAggregator(
        "a",
        new ConstantPostAggregator("", 0),
        null
    );
    postAgg.getComparator();
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ArrayOfDoublesSketchToEstimateAndBoundsPostAggregator.class)
                  .withNonnullFields("name", "field")
                  .withIgnoredFields("dependentFields")
                  .usingGetClass()
                  .verify();
  }
}
