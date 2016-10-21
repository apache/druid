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
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.IAE;
import io.druid.query.extraction.MapLookupExtractor;
import org.junit.Assert;
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

  @Test
  public void testEqualsAndHash(){
    if (retainMissing && !Strings.isNullOrEmpty(replaceMissing)) {
      // skip
      return;
    }
    final LookupExtractionFn lookupExtractionFn1 = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("foo", "bar"), false),
        retainMissing,
        replaceMissing,
        injective,
        false
    );
    final LookupExtractionFn lookupExtractionFn2 = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("foo", "bar"), false),
        retainMissing,
        replaceMissing,
        injective,
        false
    );


    final LookupExtractionFn lookupExtractionFn3 = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("foo", "bar2"), false),
        retainMissing,
        replaceMissing,
        injective,
        false
    );

    Assert.assertEquals(lookupExtractionFn1, lookupExtractionFn2);
    Assert.assertEquals(lookupExtractionFn1.hashCode(), lookupExtractionFn2.hashCode());
    Assert.assertNotEquals(lookupExtractionFn1, lookupExtractionFn3);
    Assert.assertNotEquals(lookupExtractionFn1.hashCode(), lookupExtractionFn3.hashCode());
  }

  @Test
  public void testSimpleSerDe() throws IOException
  {
    if (retainMissing && !Strings.isNullOrEmpty(replaceMissing)) {
      // skip
      return;
    }
    final LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("foo", "bar"), false),
        retainMissing,
        replaceMissing,
        injective,
        false
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
          new MapLookupExtractor(ImmutableMap.of("foo", "bar"), false),
          retainMissing,
          Strings.emptyToNull(replaceMissing),
          injective,
          false
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
        new MapLookupExtractor(ImmutableMap.of("foo", "bar"), false),
        retainMissing,
        replaceMissing,
        injective,
        false
    );

    if (Strings.isNullOrEmpty(replaceMissing) || retainMissing) {
      Assert.assertFalse(
          Arrays.equals(
              lookupExtractionFn.getCacheKey(),
              new LookupExtractionFn(
                  lookupExtractionFn.getLookup(),
                  !lookupExtractionFn.isRetainMissingValue(),
                  lookupExtractionFn.getReplaceMissingValueWith(),
                  lookupExtractionFn.isInjective(),
                  false
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
                  !lookupExtractionFn.isInjective(),
                  false
              ).getCacheKey()
          )
      );
    }
    Assert.assertFalse(
        Arrays.equals(
            lookupExtractionFn.getCacheKey(),
            new LookupExtractionFn(
                new MapLookupExtractor(weirdMap, false),
                lookupExtractionFn.isRetainMissingValue(),
                lookupExtractionFn.getReplaceMissingValueWith(),
                lookupExtractionFn.isInjective(),
                false
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
                !lookupExtractionFn.isInjective(),
                false
            ).getCacheKey()
        )
    );
  }
}
