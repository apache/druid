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

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

@RunWith(Parameterized.class)
public class FunctionalExtractionTest
{
  private static class SimpleFunctionExtraction extends FunctionalExtraction
  {
    public SimpleFunctionExtraction(
        Function<String, String> extractionFunction,
        Boolean retainMissingValue,
        String replaceMissingValueWith,
        Boolean uniqueProjections
    )
    {
      super(extractionFunction, retainMissingValue, replaceMissingValueWith, uniqueProjections);
    }

    @Override
    public byte[] getCacheKey()
    {
      return new byte[0];
    }
  }

  private static final Function<String, String> NULL_FN = new Function<String, String>()
  {
    @Nullable
    @Override
    public String apply(String input)
    {
      return null;
    }
  };

  private static final Function<String, String> TURTLE_FN = new Function<String, String>()
  {
    @Nullable
    @Override
    public String apply(@Nullable String input)
    {
      return "turtles";
    }
  };

  private static final Function<String, String> EMPTY_STR_FN = new Function<String, String>()
  {
    @Nullable
    @Override
    public String apply(@Nullable String input)
    {
      return "";
    }
  };

  private static final Function<String, String> IDENTITY_FN = new Function<String, String>()
  {
    @Nullable
    @Override
    public String apply(@Nullable String input)
    {
      return input;
    }
  };

  private static final Function<String, String> ONLY_PRESENT = new Function<String, String>()
  {
    @Nullable
    @Override
    public String apply(@Nullable String input)
    {
      return PRESENT_KEY.equals(input) ? PRESENT_VALUE : null;
    }
  };
  private static String PRESENT_KEY = "present";
  private static String PRESENT_VALUE = "present_value";
  private static String MISSING = "missing";


  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{"null", NULL_FN},
        new Object[]{"turtle", TURTLE_FN},
        new Object[]{"empty", EMPTY_STR_FN},
        new Object[]{"identity", IDENTITY_FN},
        new Object[]{"only_PRESENT", ONLY_PRESENT}
    );
  }

  private final Function<String, String> fn;

  public FunctionalExtractionTest(String label, Function<String, String> fn)
  {
    this.fn = fn;
  }

  @Test
  public void testRetainMissing()
  {
    final String in = "NOT PRESENT";
    final FunctionalExtraction exFn = new SimpleFunctionExtraction(
        fn,
        true,
        null,
        false
    );
    final String out = fn.apply(in);
    Assert.assertEquals(NullHandling.isNullOrEquivalent(out) ? in : out, exFn.apply(in));
  }

  @Test
  public void testRetainMissingButFound()
  {
    final String in = PRESENT_KEY;
    final FunctionalExtraction exFn = new SimpleFunctionExtraction(
        fn,
        true,
        null,
        false
    );
    final String out = fn.apply(in);
    Assert.assertEquals(NullHandling.isNullOrEquivalent(out) ? in : out, exFn.apply(in));
  }

  @Test
  public void testReplaceMissing()
  {
    final String in = "NOT PRESENT";
    final FunctionalExtraction exFn = new SimpleFunctionExtraction(
        fn,
        false,
        MISSING,
        false
    );
    final String out = fn.apply(in);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(NullHandling.isNullOrEquivalent(out) ? MISSING : out, exFn.apply(in));
    } else {
      Assert.assertEquals(out == null ? MISSING : out, exFn.apply(in));
    }
  }


  @Test
  public void testReplaceMissingBlank()
  {
    final String in = "NOT PRESENT";
    final FunctionalExtraction exFn = new SimpleFunctionExtraction(
        fn,
        false,
        "",
        false
    );
    final String out = fn.apply(in);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(Strings.isNullOrEmpty(out) ? null : out, exFn.apply(in));
    } else {
      Assert.assertEquals(out == null ? "" : out, exFn.apply(in));
    }
  }

  @Test
  public void testOnlyOneValuePresent()
  {
    final String in = PRESENT_KEY;
    final FunctionalExtraction exFn = new SimpleFunctionExtraction(
        fn,
        false,
        "",
        false
    );
    final String out = fn.apply(in);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(Strings.isNullOrEmpty(out) ? null : out, exFn.apply(in));
    } else {
      Assert.assertEquals(Strings.isNullOrEmpty(out) ? "" : out, exFn.apply(in));
    }
  }

  @Test
  public void testNullInputs()
  {
    final FunctionalExtraction exFn = new SimpleFunctionExtraction(
        fn,
        true,
        null,
        false
    );
    if (NullHandling.isNullOrEquivalent(fn.apply(null))) {
      Assert.assertEquals(null, exFn.apply(null));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadConfig()
  {
    @SuppressWarnings("unused") // expected exception
    final FunctionalExtraction exFn = new SimpleFunctionExtraction(
        fn,
        true,
        MISSING,
        false
    );
  }

  @Test
  public void testUniqueProjections()
  {
    Assert.assertEquals(
        ExtractionFn.ExtractionType.MANY_TO_ONE,
        new SimpleFunctionExtraction(
            fn,
            true,
            null,
            false
        ).getExtractionType()
    );
    Assert.assertEquals(
        ExtractionFn.ExtractionType.MANY_TO_ONE,
        new SimpleFunctionExtraction(
            fn,
            true,
            null,
            false
        ).getExtractionType()
    );
    Assert.assertEquals(
        ExtractionFn.ExtractionType.ONE_TO_ONE,
        new SimpleFunctionExtraction(
            fn,
            true,
            null,
            true
        ).getExtractionType()
    );
  }
}
