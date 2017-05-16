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

package io.druid.query.lookup;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class LookupsStateTest
{
  @Test
  public void testSerde() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"current\": {\n"
                     + "    \"l1\": {\n"
                     + "      \"version\": \"v1\",\n"
                     + "      \"lookupExtractorFactory\": {\n"
                     + "        \"type\": \"test\"\n"
                     + "      }\n"
                     + "    }\n"
                     + "  },\n"
                     + "  \"toLoad\": {\n"
                     + "    \"l2\": {\n"
                     + "      \"version\": \"v1\",\n"
                     + "      \"lookupExtractorFactory\": {\n"
                     + "        \"type\": \"test\"\n"
                     + "      }\n"
                     + "    }\n"
                     + "  },\n"
                     + "  \"toDrop\": [\"l3\"]\n"
                     + "}";

    TypeReference<LookupsState<LookupExtractorFactoryContainer>> typeRef =
        new TypeReference<LookupsState<LookupExtractorFactoryContainer>>()
        {
        };

    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(LookupExtractorFactoryContainerTest.TestLookupExtractorFactory.class);

    LookupsState<LookupExtractorFactoryContainer> actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, typeRef)
        ),
        typeRef
    );

    Assert.assertEquals(
        new LookupsState<>(
            ImmutableMap.of(
                "l1",
                new LookupExtractorFactoryContainer(
                    "v1",
                    new LookupExtractorFactoryContainerTest.TestLookupExtractorFactory()
                )
            ),
            ImmutableMap.of(
                "l2",
                new LookupExtractorFactoryContainer(
                    "v1",
                    new LookupExtractorFactoryContainerTest.TestLookupExtractorFactory()
                )
            ),
            ImmutableSet.of("l3")
        ),
        actual
    );
  }
}
