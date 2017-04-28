/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.lookup.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.lookup.LookupExtractorFactoryContainer;
import io.druid.query.lookup.MapLookupExtractorFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class LookupExtractorFactoryMapContainerTest
{
  private final ObjectMapper mapper;
  private final String jsonStr;
  private final LookupExtractorFactoryMapContainer testContainer;

  public LookupExtractorFactoryMapContainerTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(MapLookupExtractorFactory.class);

    jsonStr = "{\n"
              + "  \"version\": \"v1\",\n"
              + "  \"lookupExtractorFactory\": {\n"
              + "    \"type\": \"map\",\n"
              + "    \"map\": {\"k\": \"v\"},\n"
              + "    \"isOneToOne\": true\n"
              + "  }\n"
              + "}\n";

    testContainer = new LookupExtractorFactoryMapContainer(
        "v1",
        ImmutableMap.<String, Object>of(
            "type", "map",
            "map", ImmutableMap.of("k", "v"),
            "isOneToOne", true
        )
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    LookupExtractorFactoryMapContainer actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, LookupExtractorFactoryMapContainer.class)
        ),
        LookupExtractorFactoryMapContainer.class
    );
    Assert.assertEquals("v1", actual.getVersion());
    Assert.assertEquals(testContainer, actual);
  }

  @Test
  public void testReplaces() throws Exception
  {
    LookupExtractorFactoryMapContainer l0 = new LookupExtractorFactoryMapContainer(null, ImmutableMap.of());
    LookupExtractorFactoryMapContainer l1 = new LookupExtractorFactoryMapContainer(null, ImmutableMap.of());
    LookupExtractorFactoryMapContainer l2 = new LookupExtractorFactoryMapContainer("V2", ImmutableMap.of());
    LookupExtractorFactoryMapContainer l3 = new LookupExtractorFactoryMapContainer("V3", ImmutableMap.of());

    Assert.assertFalse(l0.replaces(l1));
    Assert.assertFalse(l1.replaces(l2));
    Assert.assertTrue(l2.replaces(l1));
    Assert.assertFalse(l2.replaces(l3));
    Assert.assertTrue(l3.replaces(l2));
  }

  //test interchangeability with LookupExtractorFactoryContainer
  //read and write as LookupExtractorFactoryContainer
  //then read as LookupExtractorFactoryMapContainer
  @Test
  public void testInterchangeability1() throws Exception
  {
    LookupExtractorFactoryMapContainer actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, LookupExtractorFactoryContainer.class)
        ),
        LookupExtractorFactoryMapContainer.class
    );

    Assert.assertEquals("v1", actual.getVersion());
    Assert.assertEquals(testContainer, actual);
  }

  //test interchangeability with LookupExtractorFactoryContainer
  //read and write as LookupExtractorFactoryMapContainer
  //then read as LookupExtractorFactoryContainer
  @Test
  public void testInterchangeability2() throws Exception
  {
    LookupExtractorFactoryContainer actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, LookupExtractorFactoryMapContainer.class)
        ),
        LookupExtractorFactoryContainer.class
    );

    Assert.assertEquals("v1", actual.getVersion());
    Assert.assertEquals(
        actual,
        new LookupExtractorFactoryContainer(
            "v1",
            new MapLookupExtractorFactory(ImmutableMap.of("k", "v"), true)
        )
    );
  }
}
