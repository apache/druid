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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class MapLookupExtractorFactoryTest
{
  private static final String KEY = "foo";
  private static final String VALUE = "bar";
  private static final MapLookupExtractorFactory FACTORY = new MapLookupExtractorFactory(ImmutableMap.of(KEY, VALUE), true);

  @Test
  public void testSimpleExtraction()
  {
    Assertions.assertEquals(FACTORY.get().apply(KEY), VALUE);
    Assertions.assertTrue(FACTORY.get().isOneToOne());
  }

  @Test
  public void testReplaces()
  {
    Assertions.assertFalse(FACTORY.replaces(FACTORY));
    Assertions.assertFalse(FACTORY.replaces(new MapLookupExtractorFactory(ImmutableMap.of(KEY, VALUE), true)));
    Assertions.assertTrue(FACTORY.replaces(new MapLookupExtractorFactory(ImmutableMap.of(KEY, VALUE), false)));
    Assertions.assertTrue(FACTORY.replaces(new MapLookupExtractorFactory(ImmutableMap.of(KEY + "1", VALUE), true)));
    Assertions.assertTrue(FACTORY.replaces(new MapLookupExtractorFactory(ImmutableMap.of(KEY, VALUE + "1"), true)));
    Assertions.assertTrue(FACTORY.replaces(null));
  }

  @Test
  public void testSerDeserMapLookupExtractorFactory() throws IOException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(MapLookupExtractorFactory.class);
    LookupExtractorFactory lookupExtractorFactory = new MapLookupExtractorFactory(ImmutableMap.of("key", "value"), true);
    Assertions.assertEquals(lookupExtractorFactory, mapper.readerFor(LookupExtractorFactory.class).readValue(mapper.writeValueAsString(lookupExtractorFactory)));
  }
}
