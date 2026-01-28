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
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.aggregation.PostAggregator;
import org.junit.Assert;
import org.junit.Test;


public class ArrayOfDoublesSketchConstantPostAggregatorTest
{
  @Test
  public void testSketchValue()
  {
    final String value = "AQEJAwgBzJP/////////fwIAAAAAAAAAzT6NGdX0aWUOJvS5EIhpLwAAAAAAAAAAAAAAAAAAAAA=";
    final ArrayOfDoublesSketchConstantPostAggregator postAgg = new ArrayOfDoublesSketchConstantPostAggregator(
        "constant_sketch",
        value
    );
    Assert.assertNotNull(postAgg.getSketchValue());
  }


  @Test
  public void testToString()
  {
    final PostAggregator postAgg = new ArrayOfDoublesSketchConstantPostAggregator(
        "constant_sketch",
        "AQEJAwgBzJP/////////fwIAAAAAAAAAzT6NGdX0aWUOJvS5EIhpLwAAAAAAAAAAAAAAAAAAAAA="
    );

    Assert.assertEquals(
        "ArrayOfDoublesSketchConstantPostAggregator{name='constant_sketch', value='AQEJAwgBzJP/////////fwIAAAAAAAAAzT6NGdX0aWUOJvS5EIhpLwAAAAAAAAAAAAAAAAAAAAA='}",
        postAgg.toString()
    );
  }

  @Test
  public void testComparator()
  {
    final PostAggregator postAgg = new ArrayOfDoublesSketchConstantPostAggregator(
        "constant_sketch",
        "AQEJAwgBzJP/////////fwIAAAAAAAAAzT6NGdX0aWUOJvS5EIhpLwAAAAAAAAAAAAAAAAAAAAA="
    );

    Assert.assertEquals(Comparators.alwaysEqual(), postAgg.getComparator());
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final PostAggregator there = new ArrayOfDoublesSketchConstantPostAggregator(
        "p",
        "AQEJAwgBzJP/////////fwIAAAAAAAAAzT6NGdX0aWUOJvS5EIhpLwAAAAAAAAAAAAAAAAAAAAA="
    );
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerModules(new ArrayOfDoublesSketchModule().getJacksonModules());
    PostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        PostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ArrayOfDoublesSketchConstantPostAggregator.class)
                  .usingGetClass()
                  .withNonnullFields("name")
                  .withIgnoredFields("sketchValue")
                  .verify();
  }
}

