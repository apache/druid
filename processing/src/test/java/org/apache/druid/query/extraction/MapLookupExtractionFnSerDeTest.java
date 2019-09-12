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

package org.apache.druid.query.extraction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 *
 */
public class MapLookupExtractionFnSerDeTest
{
  private static ObjectMapper mapper;
  private static final Map<String, String> RENAMES = ImmutableMap.of(
      "foo", "bar",
      "bar", "baz"
  );

  @BeforeClass
  public static void setup()
  {
    Injector defaultInjector = GuiceInjectors.makeStartupInjector();
    mapper = defaultInjector.getInstance(Key.get(ObjectMapper.class, Json.class));
  }

  @Test
  public void testDeserialization() throws IOException
  {
    final DimExtractionFn fn = mapper.readerFor(DimExtractionFn.class).readValue(
        StringUtils.format(
            "{\"type\":\"lookup\",\"lookup\":{\"type\":\"map\", \"map\":%s}}",
            mapper.writeValueAsString(RENAMES)
        )
    );
    for (String key : RENAMES.keySet()) {
      Assert.assertEquals(RENAMES.get(key), fn.apply(key));
    }
    final String crazyString = UUID.randomUUID().toString();
    Assert.assertEquals(null, fn.apply(crazyString));

    Assert.assertEquals(
        crazyString,
        mapper
            .readerFor(DimExtractionFn.class)
            .<DimExtractionFn>readValue(
                StringUtils.format(
                    "{\"type\":\"lookup\",\"lookup\":{\"type\":\"map\", \"map\":%s}, \"retainMissingValue\":true}",
                    mapper.writeValueAsString(RENAMES)
                )
            )
            .apply(crazyString)
    );
  }
}
