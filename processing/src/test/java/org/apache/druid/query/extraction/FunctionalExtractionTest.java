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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;
import java.util.stream.Stream;

@ParameterizedClass
@MethodSource("constructorFeeder")
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

  private static final Function<String, String> NULL_FN = new Function<>()
  {
    @Nullable
    @Override
    public String apply(String input)
    {
      return null;
    }
  };

  private static final Function<String, String> TURTLE_FN = new Function<>()
  {
    @Nullable
    @Override
    public String apply(@Nullable String input)
    {
      return "turtles";
    }
  };

  private static final Function<String, String> EMPTY_STR_FN = new Function<>()
  {
    @Nullable
    @Override
    public String apply(@Nullable String input)
    {
      return "";
    }
  };

  private static final Function<String, String> IDENTITY_FN = new Function<>()
  {
    @Nullable
    @Override
    public String apply(@Nullable String input)
    {
      return input;
    }
  };

  private static final Function<String, String> ONLY_PRESENT = new Function<>()
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

  public static Stream<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{"null", NULL_FN},
        new Object[]{"turtle", TURTLE_FN},
        new Object[]{"empty", EMPTY_STR_FN},
        new Object[]{"identity", IDENTITY_FN},
        new Object[]{"only_PRESENT", ONLY_PRESENT}
    ).stream();
  }

  @Parameter(0)
  public String label;
  @Parameter(1)
  public Function<String, String> fn;

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
    Assertions.assertEquals(out == null ? in : out, exFn.apply(in));
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
    Assertions.assertEquals(out == null ? in : out, exFn.apply(in));
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
    Assertions.assertEquals(out == null ? MISSING : out, exFn.apply(in));
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
    Assertions.assertEquals(out == null ? "" : out, exFn.apply(in));
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
    Assertions.assertEquals(Strings.isNullOrEmpty(out) ? "" : out, exFn.apply(in));
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
    if (fn.apply(null) == null) {
      Assertions.assertEquals(null, exFn.apply(null));
    }
  }

  @Test
  public void testBadConfig()
  {
    Assertions.assertThrows(IllegalArgumentException.class, () ->
        new SimpleFunctionExtraction(fn, true, MISSING, false)
    );
  }

  @Test
  public void testUniqueProjections()
  {
    Assertions.assertEquals(
        ExtractionFn.ExtractionType.MANY_TO_ONE,
        new SimpleFunctionExtraction(
            fn,
            true,
            null,
            false
        ).getExtractionType()
    );
    Assertions.assertEquals(
        ExtractionFn.ExtractionType.MANY_TO_ONE,
        new SimpleFunctionExtraction(
            fn,
            true,
            null,
            false
        ).getExtractionType()
    );
    Assertions.assertEquals(
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
