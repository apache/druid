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

package io.druid.query.extraction.extraction;

import com.google.common.collect.ImmutableMap;
import io.druid.query.extraction.LookupExtractionFn;
import io.druid.query.extraction.MapLookupExtractor;
import org.junit.Assert;
import org.junit.Test;

/**
 * This test suite clarifies some behavior around specific corner cases
 */
public class LookupExtractionFnExpectationsTest
{
  @Test
  public void testMissingKeyIsNull()
  {
    final LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("foo", "bar")),
        true,
        null,
        false,
        false
    );
    Assert.assertNull(lookupExtractionFn.apply(null));
  }

  @Test
  public void testMissingKeyIsReplaced()
  {
    final LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("foo", "bar")),
        false,
        "REPLACE",
        false,
        false
    );
    Assert.assertEquals("REPLACE", lookupExtractionFn.apply(null));
  }

  @Test
  public void testNullKeyIsMappable()
  {
    final LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("", "bar")),
        false,
        "REPLACE",
        false,
        false
    );
    Assert.assertEquals("bar", lookupExtractionFn.apply(null));
  }

  @Test
  public void testNullValue()
  {
    final LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("foo", "")),
        false,
        "REPLACE",
        false,
        false
    );
    Assert.assertEquals("REPLACE", lookupExtractionFn.apply(null));
  }
}
