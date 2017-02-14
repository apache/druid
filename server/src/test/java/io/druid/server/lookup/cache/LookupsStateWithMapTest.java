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
import com.google.common.collect.ImmutableSet;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.lookup.LookupExtractorFactoryContainer;
import io.druid.query.lookup.LookupsState;
import io.druid.query.lookup.MapLookupExtractorFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class LookupsStateWithMapTest
{
  private final ObjectMapper mapper;
  private final String jsonStr;
  private final LookupsStateWithMap testLookupsState;

  public LookupsStateWithMapTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(MapLookupExtractorFactory.class);

    jsonStr = "{\n"
              + "  \"current\": {\n"
              + "    \"l1\": {\n"
              + "      \"version\": \"v1\",\n"
              + "      \"lookupExtractorFactory\": {\n"
              + "        \"type\": \"map\",\n"
              + "        \"map\": {\"k\": \"v\"},\n"
              + "        \"isOneToOne\": true\n"
              + "      }\n"
              + "    }\n"
              + "  },\n"
              + "  \"toLoad\": {\n"
              + "    \"l2\": {\n"
              + "      \"version\": \"v1\",\n"
              + "      \"lookupExtractorFactory\": {\n"
              + "        \"type\": \"map\",\n"
              + "        \"map\": {\"k\": \"v\"},\n"
              + "        \"isOneToOne\": true\n"
              + "      }\n"
              + "    }\n"
              + "  },\n"
              + "  \"toDrop\": [\"l3\"]\n"
              + "}";

    testLookupsState = new LookupsStateWithMap(
        ImmutableMap.of(
            "l1",
            new LookupExtractorFactoryMapContainer(
                "v1",
                ImmutableMap.<String, Object>of(
                    "type", "map",
                    "map", ImmutableMap.of("k", "v"),
                    "isOneToOne", true
                )
            )
        ),
        ImmutableMap.of(
            "l2",
            new LookupExtractorFactoryMapContainer(
                "v1",
                ImmutableMap.<String, Object>of(
                    "type", "map",
                    "map", ImmutableMap.of("k", "v"),
                    "isOneToOne", true
                )
            )
        ),
        ImmutableSet.of("l3")
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    LookupsStateWithMap actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, LookupsStateWithMap.class)
        ),
        LookupsStateWithMap.class
    );
    Assert.assertEquals(testLookupsState, actual);
  }



  //test interchangeability with LookupsState
  //read and write as LookupsState
  //then read as LookupsStateWithMap
  @Test
  public void testInterchangeability1() throws Exception
  {
    LookupsStateWithMap actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, LookupsState.class)
        ),
        LookupsStateWithMap.class
    );

    Assert.assertEquals(testLookupsState, actual);
  }

  //test interchangeability with LookupExtractorFactoryContainer
  //read and write as LookupExtractorFactoryMapContainer
  //then read as LookupExtractorFactoryContainer
  @Test
  public void testInterchangeability2() throws Exception
  {
    LookupsState actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, LookupsStateWithMap.class)
        ),
        LookupsState.class
    );

    Assert.assertEquals(new LookupsState(
                            ImmutableMap.of(
                                "l1",
                                new LookupExtractorFactoryContainer(
                                    "v1",
                                    new MapLookupExtractorFactory(
                                        ImmutableMap.of("k", "v"),
                                        true
                                    )
                                )
                            ),
                            ImmutableMap.of(
                                "l2",
                                new LookupExtractorFactoryContainer(
                                    "v1",
                                    new MapLookupExtractorFactory(
                                        ImmutableMap.of("k", "v"),
                                        true
                                    )
                                )
                            ),
                            ImmutableSet.of("l3")
                        ),
                        actual
    );
  }
}
