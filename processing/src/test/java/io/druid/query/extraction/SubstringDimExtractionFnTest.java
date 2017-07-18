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

package io.druid.query.extraction;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 */
public class SubstringDimExtractionFnTest
{
  @Test
  public void testSubstrings()
  {
    ExtractionFn extractionFn = new SubstringDimExtractionFn(1, 3);

    Assert.assertEquals("ppl", extractionFn.apply("apple"));
    Assert.assertEquals("e", extractionFn.apply("be"));
    Assert.assertEquals("ool", extractionFn.apply("cool"));
    Assert.assertEquals(null, extractionFn.apply("a"));
  }

  @Test (expected = IllegalArgumentException.class)
  public void testZeroLength()
  {
    ExtractionFn extractionFnNoLength = new SubstringDimExtractionFn(1,0);
  }

  @Test
  public void testNoLength()
  {
    ExtractionFn extractionFnNoLength = new SubstringDimExtractionFn(1,null);

    // 0 length substring returns remainder
    Assert.assertEquals("abcdef", extractionFnNoLength.apply("/abcdef"));

    // 0 length substring empty result is null
    Assert.assertEquals(null, extractionFnNoLength.apply("/"));
  }

  @Test
  public void testGetCacheKey()
  {
    ExtractionFn extractionFn1 = new SubstringDimExtractionFn(2,4);
    ExtractionFn extractionFn2 = new SubstringDimExtractionFn(2,4);
    ExtractionFn extractionFn3 = new SubstringDimExtractionFn(1,4);

    Assert.assertArrayEquals(extractionFn1.getCacheKey(), extractionFn2.getCacheKey());

    Assert.assertFalse(Arrays.equals(extractionFn1.getCacheKey(), extractionFn3.getCacheKey()));
  }

  @Test
  public void testHashCode()
  {
    ExtractionFn extractionFn1 = new SubstringDimExtractionFn(2,4);
    ExtractionFn extractionFn2 = new SubstringDimExtractionFn(2,4);
    ExtractionFn extractionFn3 = new SubstringDimExtractionFn(1,4);

    Assert.assertEquals(extractionFn1.hashCode(), extractionFn2.hashCode());

    Assert.assertNotEquals(extractionFn1.hashCode(), extractionFn3.hashCode());
  }

  @Test
  public void testNullAndEmpty()
  {
    ExtractionFn extractionFn = new SubstringDimExtractionFn(2,4);
    // no match, map empty input value to null
    Assert.assertEquals(null, extractionFn.apply(""));
    // null value, returns null
    Assert.assertEquals(null, extractionFn.apply(null));
    // empty match, map empty result to null
    Assert.assertEquals(null, extractionFn.apply("/a"));
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    
    final String json = "{ \"type\" : \"substring\", \"index\" : 1, \"length\" : 3 }";
    final String jsonNoLength = "{ \"type\" : \"substring\", \"index\" : 1 }";

    SubstringDimExtractionFn extractionFn = (SubstringDimExtractionFn) objectMapper.readValue(json, ExtractionFn.class);
    SubstringDimExtractionFn extractionFnNoLength = (SubstringDimExtractionFn) objectMapper.readValue(jsonNoLength, ExtractionFn.class);

    Assert.assertEquals(1, extractionFn.getIndex());
    Assert.assertEquals(new Integer(3), extractionFn.getLength());
    Assert.assertEquals(1, extractionFnNoLength.getIndex());
    Assert.assertEquals(null, extractionFnNoLength.getLength());

    // round trip
    Assert.assertEquals(
        extractionFn,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFn),
            ExtractionFn.class
        )
    );

    Assert.assertEquals(
        extractionFnNoLength,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFnNoLength),
            ExtractionFn.class
        )
    );
  }
}
