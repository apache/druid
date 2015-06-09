/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.query.extraction.extraction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.IAE;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.extraction.LookupExtractionFn;
import io.druid.query.extraction.MapLookupExtractor;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class LookupExtractionFnTest
{
  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder()
  {
    return Iterables.transform(
        Sets.cartesianProduct(
            ImmutableList.of(
                ImmutableSet.of(true, false),
                ImmutableSet.of("", "MISSING VALUE"),
                ImmutableSet.of(true, false)
            )
        ), new Function<List<?>, Object[]>()
        {
          @Nullable
          @Override
          public Object[] apply(List<?> input)
          {
            return input.toArray();
          }
        }
    );
  }

  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();
  private final boolean retainMissing;
  private final String replaceMissing;
  private final boolean injective;

  public LookupExtractionFnTest(boolean retainMissing, String replaceMissing, boolean injective)
  {
    this.replaceMissing = Strings.emptyToNull(replaceMissing);
    this.retainMissing = retainMissing;
    this.injective = injective;
  }

  @BeforeClass
  public static void setUpStatic()
  {
    OBJECT_MAPPER.registerSubtypes(LookupExtractionFn.class);
  }

  @Test
  public void testSimpleSerDe() throws IOException
  {
    if (retainMissing && !Strings.isNullOrEmpty(replaceMissing)) {
      // skip
      return;
    }
    final LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("foo", "bar")),
        retainMissing,
        replaceMissing,
        injective
    );
    final String str1 = OBJECT_MAPPER.writeValueAsString(lookupExtractionFn);

    final LookupExtractionFn lookupExtractionFn2 = OBJECT_MAPPER.readValue(str1, LookupExtractionFn.class);

    Assert.assertEquals(retainMissing, lookupExtractionFn2.isRetainMissingValue());
    Assert.assertEquals(replaceMissing, lookupExtractionFn2.getReplaceMissingValueWith());
    Assert.assertEquals(injective, lookupExtractionFn2.isInjective());

    Assert.assertArrayEquals(lookupExtractionFn.getCacheKey(), lookupExtractionFn2.getCacheKey());

    Assert.assertEquals(
        str1,
        OBJECT_MAPPER.writeValueAsString(lookupExtractionFn2)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalArgs()
  {
    if (retainMissing && !Strings.isNullOrEmpty(replaceMissing)) {
      final LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(
          new MapLookupExtractor(ImmutableMap.of("foo", "bar")),
          retainMissing,
          Strings.emptyToNull(replaceMissing),
          injective
      );
    } else {
      throw new IAE("Case not valid");
    }
  }

  @Test
  public void testCacheKey()
  {
    if (retainMissing && !Strings.isNullOrEmpty(replaceMissing)) {
      // skip
      return;
    }
    final Map<String, String> weirdMap = Maps.newHashMap();
    weirdMap.put("foobar", null);

    final LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("foo", "bar")),
        retainMissing,
        replaceMissing,
        injective
    );

    if (Strings.isNullOrEmpty(replaceMissing) || retainMissing) {
      Assert.assertFalse(
          Arrays.equals(
              lookupExtractionFn.getCacheKey(),
              new LookupExtractionFn(
                  lookupExtractionFn.getLookup(),
                  !lookupExtractionFn.isRetainMissingValue(),
                  lookupExtractionFn.getReplaceMissingValueWith(),
                  lookupExtractionFn.isInjective()
              ).getCacheKey()
          )
      );
      Assert.assertFalse(
          Arrays.equals(
              lookupExtractionFn.getCacheKey(),
              new LookupExtractionFn(
                  lookupExtractionFn.getLookup(),
                  !lookupExtractionFn.isRetainMissingValue(),
                  lookupExtractionFn.getReplaceMissingValueWith(),
                  !lookupExtractionFn.isInjective()
              ).getCacheKey()
          )
      );
    }
    Assert.assertFalse(
        Arrays.equals(
            lookupExtractionFn.getCacheKey(),
            new LookupExtractionFn(
                new MapLookupExtractor(weirdMap),
                lookupExtractionFn.isRetainMissingValue(),
                lookupExtractionFn.getReplaceMissingValueWith(),
                lookupExtractionFn.isInjective()
            ).getCacheKey()
        )
    );
    Assert.assertFalse(
        Arrays.equals(
            lookupExtractionFn.getCacheKey(),
            new LookupExtractionFn(
                lookupExtractionFn.getLookup(),
                lookupExtractionFn.isRetainMissingValue(),
                lookupExtractionFn.getReplaceMissingValueWith(),
                !lookupExtractionFn.isInjective()
            ).getCacheKey()
        )
    );
  }
}
