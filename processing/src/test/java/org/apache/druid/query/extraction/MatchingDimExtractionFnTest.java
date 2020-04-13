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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 */
public class MatchingDimExtractionFnTest
{
  private static final String[] TEST_STRINGS = {
      "Quito",
      "Calgary",
      "Tokyo",
      "Stockholm",
      "Vancouver",
      "Pretoria",
      "Wellington",
      null,
      "Ontario"
  };

  @Test
  public void testExtraction()
  {
    String regex = ".*[Tt][Oo].*";
    ExtractionFn extractionFn = new MatchingDimExtractionFn(regex);
    List<String> expected = Arrays.asList("Quito", "Tokyo", "Stockholm", "Pretoria", "Wellington");
    Set<String> extracted = new HashSet<>();

    for (String str : TEST_STRINGS) {
      String res = extractionFn.apply(str);
      if (res != null) {
        extracted.add(res);
      }
    }

    Assert.assertEquals(5, extracted.size());

    for (String str : extracted) {
      Assert.assertTrue(expected.contains(str));
    }
  }

  @Test
  public void testNullExtraction()
  {
    String regex = "^$";
    ExtractionFn extractionFn = new MatchingDimExtractionFn(regex);

    Assert.assertNull(extractionFn.apply((Object) null));
    Assert.assertNull(extractionFn.apply((String) null));
    Assert.assertEquals(NullHandling.replaceWithDefault() ? null : "", extractionFn.apply((String) ""));
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"type\" : \"partial\", \"expr\" : \".(...)?\" }";
    MatchingDimExtractionFn extractionFn = (MatchingDimExtractionFn) objectMapper.readValue(json, ExtractionFn.class);

    Assert.assertEquals(".(...)?", extractionFn.getExpr());

    // round trip
    Assert.assertEquals(
        extractionFn,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFn),
            ExtractionFn.class
        )
    );
  }
}
