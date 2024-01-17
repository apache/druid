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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class LookupExtractorTest extends InitializedNullHandlingTest
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

  static final Set<String> EXPECTED_REVERSE_SET = ImmutableSet.of("key1", "key-1", "key2", "");
  LookupExtractor lookupExtractor = new MapLookupExtractor(EXPECTED_MAP, false);

  @Test
  public void testSerDes() throws IOException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    Assert.assertEquals(
        lookupExtractor,
        mapper.readerFor(LookupExtractor.class).readValue(mapper.writeValueAsBytes(lookupExtractor))
    );
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
    Assert.assertEquals(Collections.emptyMap(), lookupExtractor.applyAll(Collections.emptySet()));
  }

  @Test
  public void testApplyAllWithNotExisting()
  {
    Map<String, String> expected = new HashMap<>();
    expected.put("not there", null);
    Assert.assertEquals(expected, lookupExtractor.applyAll(Collections.singletonList("not there")));
  }

  @Test
  public void testUnapplyAllWithEmptySet()
  {
    Assert.assertEquals(Collections.emptySet(), toSet(lookupExtractor.unapplyAll(Collections.emptySet())));
  }

  @Test
  public void testUnapplyAll()
  {
    Assert.assertEquals(EXPECTED_REVERSE_SET, toSet(lookupExtractor.unapplyAll(new HashSet<>(EXPECTED_MAP.values()))));
  }

  @Nullable
  private static <T> Set<T> toSet(@Nullable final Iterator<T> iterator)
  {
    if (iterator == null) {
      return null;
    } else {
      return Sets.newHashSet(iterator);
    }
  }
}
