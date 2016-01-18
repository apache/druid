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

package io.druid.query.extraction.namespace;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import io.druid.query.extraction.NamespacedExtractor;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class NamespacedExtractorTest
{
  private static final ConcurrentMap<String, Function<String, String>> defaultMap = new ConcurrentHashMap<>();
  private static final Function<String, String> NOOP_FN = new Function<String, String>()
  {
    @Nullable
    @Override
    public String apply(@Nullable String input)
    {
      return Strings.isNullOrEmpty(input) ? null : input;
    }
  };

  private static final Function<String, List<String>> NOOP_REVERSE_FN = new Function<String, List<String>>()
  {
    @Nullable
    @Override
    public List<String> apply(@Nullable String input)
    {
      return Strings.isNullOrEmpty(input) ? Collections.<String>emptyList() : Arrays.asList(input);
    }
  };

  private static final Function<String, Function<String, String>> defaultFnFinder = new Function<String, Function<String, String>>()
  {
    @Nullable
    @Override
    public Function<String, String> apply(@Nullable String input)
    {
      Function<String, String> fn = defaultMap.get(input);
      return fn == null ? NOOP_FN : fn;
    }
  };

  private static final Function<String,Function<String, List<String>>> defaultReverseFnFinder = new Function<String, Function<String,List<String>>>()
  {
    @Nullable
    @Override
    public Function<String, java.util.List<String>> apply(@Nullable final String value)
    {
      return NOOP_REVERSE_FN;
    }
  };

  @BeforeClass
  public static void setupStatic()
  {
    defaultMap.put(
        "noop", new Function<String, String>()
        {
          @Nullable
          @Override
          public String apply(String input)
          {
            return input;
          }
        }
    );
    defaultMap.put(
        "null", new Function<String, String>()
        {
          @Nullable
          @Override
          public String apply(@Nullable String input)
          {
            return null;
          }
        }
    );
    defaultMap.put(
        "turtles", new Function<String, String>()
        {
          @Nullable
          @Override
          public String apply(@Nullable String input)
          {
            return "turtle";
          }
        }
    );
    defaultMap.put(
        "empty", new Function<String, String>()
        {
          @Nullable
          @Override
          public String apply(@Nullable String input)
          {
            return "";
          }
        }
    );
  }

  @Test
  public void testSimpleNamespace()
  {
    NamespacedExtractor namespacedExtractor = new NamespacedExtractor(defaultFnFinder, defaultReverseFnFinder, "noop");
    for (int i = 0; i < 10; ++i) {
      final String val = UUID.randomUUID().toString();
      Assert.assertEquals(val, namespacedExtractor.apply(val));
      Assert.assertEquals(Arrays.asList(val), namespacedExtractor.unapply(val));
    }
    Assert.assertEquals("", namespacedExtractor.apply(""));
    Assert.assertNull(namespacedExtractor.apply(null));
    Assert.assertEquals(Collections.emptyList(), namespacedExtractor.unapply(null));
    Assert.assertEquals("The awesomeness", namespacedExtractor.apply("The awesomeness"));
  }

  @Test
  public void testUnknownNamespace()
  {
    NamespacedExtractor namespacedExtractor = new NamespacedExtractor(
        defaultFnFinder,
        defaultReverseFnFinder,
        "HFJDKSHFUINEWUINIUENFIUENFUNEWI"
    );
    for (int i = 0; i < 10; ++i) {
      final String val = UUID.randomUUID().toString();
      Assert.assertEquals(val, namespacedExtractor.apply(val));
    }
    Assert.assertNull(namespacedExtractor.apply(""));
    Assert.assertNull(namespacedExtractor.apply(null));
  }

  @Test
  public void testTurtles()
  {
    NamespacedExtractor namespacedExtractor = new NamespacedExtractor(defaultFnFinder, defaultReverseFnFinder, "turtles");
    for (int i = 0; i < 10; ++i) {
      final String val = UUID.randomUUID().toString();
      Assert.assertEquals("turtle", namespacedExtractor.apply(val));
    }
    Assert.assertEquals("turtle", namespacedExtractor.apply(""));
    Assert.assertEquals("turtle", namespacedExtractor.apply(null));
  }

  @Test
  public void testEmpty()
  {
    NamespacedExtractor namespacedExtractor = new NamespacedExtractor(defaultFnFinder, defaultReverseFnFinder, "empty");
    Assert.assertEquals("", namespacedExtractor.apply(""));
    Assert.assertEquals("", namespacedExtractor.apply(null));
    Assert.assertEquals("", namespacedExtractor.apply("anything"));
  }

  @Test
  public void testNull()
  {
    NamespacedExtractor namespacedExtractor = new NamespacedExtractor(defaultFnFinder, defaultReverseFnFinder, "null");
    Assert.assertNull(namespacedExtractor.apply(""));
    Assert.assertNull(namespacedExtractor.apply(null));
  }

  @Test
  public void testBlankMissingValueIsNull()
  {
    NamespacedExtractor namespacedExtractor = new NamespacedExtractor(defaultFnFinder, defaultReverseFnFinder, "null");
    Assert.assertNull(namespacedExtractor.apply("fh43u1i2"));
    Assert.assertNull(namespacedExtractor.apply(""));
    Assert.assertNull(namespacedExtractor.apply(null));
  }
}
