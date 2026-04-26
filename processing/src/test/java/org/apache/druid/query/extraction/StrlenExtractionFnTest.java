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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StrlenExtractionFnTest
{
  @Test
  public void testApply()
  {
    Assertions.assertNull(StrlenExtractionFn.instance().apply(null));
    Assertions.assertEquals("0", StrlenExtractionFn.instance().apply(""));
    Assertions.assertEquals("1", StrlenExtractionFn.instance().apply("x"));
    Assertions.assertEquals("3", StrlenExtractionFn.instance().apply("foo"));
    Assertions.assertEquals("3", StrlenExtractionFn.instance().apply("föo"));
    Assertions.assertEquals("2", StrlenExtractionFn.instance().apply("\uD83D\uDE02"));
    Assertions.assertEquals("1", StrlenExtractionFn.instance().apply(1));
    Assertions.assertEquals("2", StrlenExtractionFn.instance().apply(-1));
  }

  @Test
  public void testGetCacheKey()
  {
    Assertions.assertArrayEquals(StrlenExtractionFn.instance().getCacheKey(), StrlenExtractionFn.instance().getCacheKey());
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();

    final String json = "{ \"type\" : \"strlen\" }";

    StrlenExtractionFn extractionFn = (StrlenExtractionFn) objectMapper.readValue(json, ExtractionFn.class);
    StrlenExtractionFn extractionFnRoundTrip = (StrlenExtractionFn) objectMapper.readValue(
        objectMapper.writeValueAsString(extractionFn),
        ExtractionFn.class
    );

    // Should all actually be the same instance.
    Assertions.assertTrue(extractionFn == extractionFnRoundTrip);
    Assertions.assertTrue(extractionFn == StrlenExtractionFn.instance());
  }

  @Test
  public void testEqualsContract()
  {
    EqualsVerifier.forClass(StrlenExtractionFn.class).verify();
  }
}
