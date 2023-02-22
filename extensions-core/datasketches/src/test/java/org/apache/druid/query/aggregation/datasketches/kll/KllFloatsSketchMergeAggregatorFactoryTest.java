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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class KllFloatsSketchMergeAggregatorFactoryTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(KllFloatsSketchMergeAggregatorFactory.class)
                  .withNonnullFields("name", "fieldName")
                  .withIgnoredFields("cacheTypeId")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(
        new NamedType(KllFloatsSketchMergeAggregatorFactory.class, KllSketchModule.FLOATS_SKETCH_MERGE)
    );
    final KllFloatsSketchMergeAggregatorFactory factory = new KllFloatsSketchMergeAggregatorFactory(
        "myFactory",
        1024,
        1000L
    );
    final byte[] json = mapper.writeValueAsBytes(factory);
    final KllFloatsSketchMergeAggregatorFactory fromJson = (KllFloatsSketchMergeAggregatorFactory) mapper.readValue(
        json,
        AggregatorFactory.class
    );
    Assert.assertEquals(factory, fromJson);
  }
}
