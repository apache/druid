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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 */
public class SubstringDimExtractionFnTest
{
  @Test
  public void testSubstrings()
  {
    ExtractionFn extractionFn = new SubstringDimExtractionFn(1, 3);

    Assertions.assertEquals("ppl", extractionFn.apply("apple"));
    Assertions.assertEquals("e", extractionFn.apply("be"));
    Assertions.assertEquals("ool", extractionFn.apply("cool"));
    Assertions.assertNull(extractionFn.apply("a"));
  }

  @Test
  public void testZeroLength()
  {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new SubstringDimExtractionFn(1, 0));
  }

  @Test
  public void testNoLength()
  {
    ExtractionFn extractionFnNoLength = new SubstringDimExtractionFn(1, null);

    // 0 length substring returns remainder
    Assertions.assertEquals("abcdef", extractionFnNoLength.apply("/abcdef"));

    // 0 length substring empty result is null
    Assertions.assertNull(extractionFnNoLength.apply("/"));
  }

  @Test
  public void testGetCacheKey()
  {
    ExtractionFn extractionFn1 = new SubstringDimExtractionFn(2, 4);
    ExtractionFn extractionFn2 = new SubstringDimExtractionFn(2, 4);
    ExtractionFn extractionFn3 = new SubstringDimExtractionFn(1, 4);

    Assertions.assertArrayEquals(extractionFn1.getCacheKey(), extractionFn2.getCacheKey());

    Assertions.assertFalse(Arrays.equals(extractionFn1.getCacheKey(), extractionFn3.getCacheKey()));
  }

  @Test
  public void testHashCode()
  {
    ExtractionFn extractionFn1 = new SubstringDimExtractionFn(2, 4);
    ExtractionFn extractionFn2 = new SubstringDimExtractionFn(2, 4);
    ExtractionFn extractionFn3 = new SubstringDimExtractionFn(1, 4);

    Assertions.assertEquals(extractionFn1.hashCode(), extractionFn2.hashCode());

    Assertions.assertNotEquals(extractionFn1.hashCode(), extractionFn3.hashCode());
  }

  @Test
  public void testNullAndEmpty()
  {
    ExtractionFn extractionFn = new SubstringDimExtractionFn(2, 4);
    // no match, map empty input value to null
    Assertions.assertNull(extractionFn.apply(""));
    // null value, returns null
    Assertions.assertNull(extractionFn.apply(null));
    // empty match, map empty result to null
    Assertions.assertNull(extractionFn.apply("/a"));
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    
    final String json = "{ \"type\" : \"substring\", \"index\" : 1, \"length\" : 3 }";
    final String jsonNoLength = "{ \"type\" : \"substring\", \"index\" : 1 }";

    SubstringDimExtractionFn extractionFn = (SubstringDimExtractionFn) objectMapper.readValue(json, ExtractionFn.class);
    SubstringDimExtractionFn extractionFnNoLength = (SubstringDimExtractionFn) objectMapper.readValue(jsonNoLength, ExtractionFn.class);

    Assertions.assertEquals(1, extractionFn.getIndex());
    Assertions.assertEquals(Integer.valueOf(3), extractionFn.getLength());
    Assertions.assertEquals(1, extractionFnNoLength.getIndex());
    Assertions.assertNull(extractionFnNoLength.getLength());

    // round trip
    Assertions.assertEquals(
        extractionFn,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFn),
            ExtractionFn.class
        )
    );

    Assertions.assertEquals(
        extractionFnNoLength,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFnNoLength),
            ExtractionFn.class
        )
    );
  }
}
