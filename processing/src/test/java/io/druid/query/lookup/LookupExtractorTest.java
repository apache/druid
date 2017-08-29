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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.extraction.MapLookupExtractor;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LookupExtractorTest
{

  static final Map<String, String> EXPECTED_MAP = ImmutableMap.of(
      "key1",
      "value1",
      "key2",
      "value2",
      "key-1",
      "value1",
      "",
      "emptyString"
  );

  static final Map<String, List<String>> EXPECTED_REVERSE_MAP = ImmutableMap.of(
      "value1",
      Arrays.asList("key1", "key-1"),
      "value2",
      Arrays.asList("key2"),
      "emptyString",
      Arrays.asList("")
  );
  LookupExtractor lookupExtractor = new MapLookupExtractor(EXPECTED_MAP, false);

  @Test
  public void testSerDes() throws IOException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    Assert.assertEquals(lookupExtractor, mapper.reader(LookupExtractor.class).readValue(mapper.writeValueAsBytes(lookupExtractor)));
  }

  @Test
  public void testApplyAll()
  {
    Assert.assertEquals(EXPECTED_MAP, lookupExtractor.applyAll(EXPECTED_MAP.keySet()));
  }

  @Test
  public void testApplyAllWithNull()
  {
    Assert.assertEquals(Collections.emptyMap(), lookupExtractor.applyAll(null));
  }

  @Test
  public void testApplyAllWithEmptySet()
  {
    Assert.assertEquals(Collections.emptyMap(), lookupExtractor.applyAll(Collections.<String>emptySet()));
  }

  @Test
  public void testApplyAllWithNotExisting()
  {
    Map<String, String> expected = new HashMap<>();
    expected.put("not there", null);
    Assert.assertEquals(expected, lookupExtractor.applyAll(Lists.newArrayList("not there")));
  }

  @Test
  public void testUnapplyAllWithNull()
  {
    Assert.assertEquals(Collections.emptyMap(), lookupExtractor.unapplyAll(null));
  }

  @Test
  public void testunapplyAllWithEmptySet()
  {
    Assert.assertEquals(Collections.emptyMap(), lookupExtractor.unapplyAll(Collections.<String>emptySet()));
  }

  @Test
  public void testUnapplyAll()
  {
    Assert.assertEquals(EXPECTED_REVERSE_MAP, lookupExtractor.unapplyAll(EXPECTED_MAP.values()));
  }
}
