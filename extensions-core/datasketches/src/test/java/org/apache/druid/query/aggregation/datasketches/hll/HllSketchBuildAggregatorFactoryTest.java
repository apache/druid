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

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class HllSketchBuildAggregatorFactoryTest
{
  private final ObjectMapper jsonMapper;

  public HllSketchBuildAggregatorFactoryTest()
  {
    this.jsonMapper = TestHelper.makeJsonMapper().copy();
    jsonMapper.registerModules(new HllSketchModule().getJacksonModules());
  }

  @Test
  public void testSerde() throws IOException
  {
    final HllSketchBuildAggregatorFactory factory = new HllSketchBuildAggregatorFactory(
        "foo",
        "bar",
        18,
        TgtHllType.HLL_8.name(),
        StringEncoding.UTF8,
        true
    );

    final String serializedString = jsonMapper.writeValueAsString(factory);

    Assert.assertEquals(
        "{\"type\":\"HLLSketchBuild\",\"name\":\"foo\",\"fieldName\":\"bar\",\"lgK\":18,\"tgtHllType\":\"HLL_8\",\"stringEncoding\":\"utf8\",\"round\":true}",
        serializedString
    );

    final AggregatorFactory factory2 = jsonMapper.readValue(
        serializedString,
        AggregatorFactory.class
    );

    Assert.assertEquals(factory, factory2);
  }

  @Test
  public void testSerdeWithDefaults() throws IOException
  {
    final HllSketchBuildAggregatorFactory factory = new HllSketchBuildAggregatorFactory(
        "foo",
        "bar",
        null,
        null,
        null,
        false
    );

    final String serializedString = jsonMapper.writeValueAsString(factory);

    Assert.assertEquals(
        "{\"type\":\"HLLSketchBuild\","
        + "\"name\":\"foo\","
        + "\"fieldName\":\"bar\","
        + "\"lgK\":12,"
        + "\"tgtHllType\":\"HLL_4\","
        + "\"round\":false"
        + "}",
        serializedString
    );

    final AggregatorFactory factory2 = jsonMapper.readValue(
        serializedString,
        AggregatorFactory.class
    );

    Assert.assertEquals(factory, factory2);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(HllSketchBuildAggregatorFactory.class).usingGetClass().verify();
  }
}
