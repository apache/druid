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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.server.DruidNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class LookupUtilsTest
{
  private static final TypeReference<Map<String, Object>> LOOKUPS_ALL_GENERIC_REFERENCE =
      new TypeReference<Map<String, Object>>()
      {
      };

  private static final TypeReference<Map<String, LookupExtractorFactoryContainer>> LOOKUPS_ALL_REFERENCE =
      new TypeReference<Map<String, LookupExtractorFactoryContainer>>()
      {
      };

  private static final String LOOKUP_VALID_INNER = "  \"lookup_uri_good\": {\n"
                                                   + "    \"version\": \"2021-12-03T01:04:05.151Z\",\n"
                                                   + "    \"lookupExtractorFactory\": {\n"
                                                   + "      \"type\": \"cachedNamespace\",\n"
                                                   + "      \"extractionNamespace\": {\n"
                                                   + "        \"type\": \"uri\",\n"
                                                   + "        \"uri\": \"file:///home/lookup_data.json\",\n"
                                                   + "        \"namespaceParseSpec\": {\n"
                                                   + "          \"format\": \"simpleJson\"\n"
                                                   + "        },\n"
                                                   + "        \"pollPeriod\": \"PT30S\",\n"
                                                   + "        \"maxHeapPercentage\": 1\n"
                                                   + "      }\n"
                                                   + "    }\n"
                                                   + "  }";


  private static final String LOOKUP_VALID = "{\n"
                                             + LOOKUP_VALID_INNER + "\n"
                                             + "}";

  private static final String LOOKUP_WITH_KEY_COLUMN_BUT_NO_VALUE_COLUMN_INNER =
      "  \"lookup_keyColumn_but_no_valueColumn\": {\n"
      + "    \"version\": \"2021-12-03T02:17:01.983Z\",\n"
      + "    \"lookupExtractorFactory\": {\n"
      + "      \"type\": \"cachedNamespace\",\n"
      + "      \"extractionNamespace\": {\n"
      + "        \"type\": \"uri\",\n"
      + "        \"fileRegex\": \".*csv\",\n"
      + "        \"uriPrefix\": \"s3://bucket/path/\",\n"
      + "        \"namespaceParseSpec\": {\n"
      + "          \"format\": \"csv\",\n"
      + "          \"columns\": [\n"
      + "            \"cluster_id\",\n"
      + "            \"account_id\",\n"
      + "            \"manager_host\"\n"
      + "          ],\n"
      + "          \"keyColumn\": \"cluster_id\",\n"
      + "          \"hasHeaderRow\": true,\n"
      + "          \"skipHeaderRows\": 1\n"
      + "        },\n"
      + "        \"pollPeriod\": \"PT30S\"\n"
      + "      }\n"
      + "    }\n"
      + "  }";

  private static final String LOOKUP_WITH_KEY_COLUMN_BUT_NO_VALUE_COLUMN =
      "{\n"
      + LOOKUP_WITH_KEY_COLUMN_BUT_NO_VALUE_COLUMN_INNER + "\n"
      + "}";

  private static final String LOOKSUPS_INVALID_AND_VALID = "{\n"
                                                      + LOOKUP_WITH_KEY_COLUMN_BUT_NO_VALUE_COLUMN_INNER + ",\n"
                                                      + LOOKUP_VALID_INNER + "\n"
                                                      + "}";
  private ObjectMapper mapper;

  @Before
  public void setup()
  {
    final Injector injector = makeInjector();
    mapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    mapper.registerSubtypes(NamespaceLookupExtractorFactory.class);
  }

  @Test
  public void test_tryConvertObjectMapToLookupConfigMap_allValid() throws IOException
  {
    mapper.registerSubtypes(NamespaceLookupExtractorFactory.class);
    Map<String, LookupExtractorFactoryContainer> validLookupExpected = mapper.readValue(
        LOOKUP_VALID,
        LOOKUPS_ALL_REFERENCE);

    Map<String, Object> validLookupGeneric = mapper.readValue(
        LOOKUP_VALID,
        LOOKUPS_ALL_GENERIC_REFERENCE);
    Map<String, LookupExtractorFactoryContainer> actualLookup =
        LookupUtils.tryConvertObjectMapToLookupConfigMap(validLookupGeneric, mapper);

    Assert.assertEquals(mapper.writeValueAsString(validLookupExpected), mapper.writeValueAsString(actualLookup));
  }

  @Test
  public void test_tryConvertObjectMapToLookupConfigMap_allInvalid_emptyMap()
      throws IOException
  {
    mapper.registerSubtypes(NamespaceLookupExtractorFactory.class);

    Map<String, Object> validLookupGeneric = mapper.readValue(
        LOOKUP_WITH_KEY_COLUMN_BUT_NO_VALUE_COLUMN,
        LOOKUPS_ALL_GENERIC_REFERENCE);
    Map<String, LookupExtractorFactoryContainer> actualLookup =
        LookupUtils.tryConvertObjectMapToLookupConfigMap(validLookupGeneric, mapper);

    Assert.assertTrue(actualLookup.isEmpty());
  }

  @Test
  public void test_tryConvertObjectMapToLookupConfigMap_goodAndBadConfigs_skipsBad()
      throws IOException
  {
    mapper.registerSubtypes(NamespaceLookupExtractorFactory.class);
    Map<String, LookupExtractorFactoryContainer> validLookupExpected = mapper.readValue(
        LOOKUP_VALID,
        LOOKUPS_ALL_REFERENCE);

    Map<String, Object> validLookupGeneric = mapper.readValue(
        LOOKSUPS_INVALID_AND_VALID,
        LOOKUPS_ALL_GENERIC_REFERENCE);
    Map<String, LookupExtractorFactoryContainer> actualLookup =
        LookupUtils.tryConvertObjectMapToLookupConfigMap(validLookupGeneric, mapper);

    Assert.assertEquals(mapper.writeValueAsString(validLookupExpected), mapper.writeValueAsString(actualLookup));
  }

  private Injector makeInjector()
  {
    return Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder,
                    Key.get(DruidNode.class, Self.class),
                    new DruidNode("test-inject", null, false, null, null, true, false)
                );
              }
            }
        )
    );
  }
}
