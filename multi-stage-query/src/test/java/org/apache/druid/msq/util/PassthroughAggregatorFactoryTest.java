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

package org.apache.druid.msq.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PassthroughAggregatorFactoryTest
{

  @Test
  public void testSerde() throws JsonProcessingException
  {
    ObjectMapper objectMapper = new DefaultObjectMapper();
    objectMapper.registerSubtypes(PassthroughAggregatorFactory.class);
    AggregatorFactory aggregatorFactory = new PassthroughAggregatorFactory("x", "y");
    String serializedvalue = objectMapper.writeValueAsString(aggregatorFactory);
    Assertions.assertEquals(
        "{\"type\":\"passthrough\",\"columnName\":\"x\",\"complexTypeName\":\"y\"}",
        serializedvalue
    );
    Assertions.assertEquals(aggregatorFactory, objectMapper.readValue(serializedvalue, AggregatorFactory.class));
  }

  @Test
  public void testRequiredFields()
  {
    Assertions.assertEquals(
        ImmutableList.of("x"),
        new PassthroughAggregatorFactory("x", "y").requiredFields()
    );
  }

  @Test
  public void testGetCombiningFactory()
  {
    Assertions.assertEquals(
        new PassthroughAggregatorFactory("x", "y"),
        new PassthroughAggregatorFactory("x", "y").getCombiningFactory()
    );
  }

  @Test
  public void testGetMergingFactoryOk() throws AggregatorFactoryNotMergeableException
  {
    final AggregatorFactory mergingFactory =
        new PassthroughAggregatorFactory("x", "y").getMergingFactory(new PassthroughAggregatorFactory("x", "y"));

    Assertions.assertEquals(new PassthroughAggregatorFactory("x", "y"), mergingFactory);
  }

  @Test
  public void testGetMergingFactoryNotOk()
  {
    Assertions.assertThrows(
        AggregatorFactoryNotMergeableException.class,
        () -> new PassthroughAggregatorFactory("x", "y").getMergingFactory(new PassthroughAggregatorFactory("x", "z"))
    );

    Assertions.assertThrows(
        AggregatorFactoryNotMergeableException.class,
        () -> new PassthroughAggregatorFactory("x", "y").getMergingFactory(new PassthroughAggregatorFactory("z", "y"))
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(PassthroughAggregatorFactory.class).usingGetClass().verify();
  }
}
