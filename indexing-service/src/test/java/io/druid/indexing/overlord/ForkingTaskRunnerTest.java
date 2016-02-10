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

package io.druid.indexing.overlord;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import org.apache.commons.exec.CommandLine;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ForkingTaskRunnerTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  // This tests the test to make sure the test fails when it should.
  @Test(expected = AssertionError.class)
  public void testPatternMatcherFailureForJavaOptions()
  {
    checkValues(new String[]{"not quoted has space"});
  }

  @Test // Command line cannot be empty
  public void testPatternMatcherFailureForSpaceOnlyJavaOptions()
  {
    expectedException.expect(new BaseMatcher<Object>()
    {
      @Override
      public boolean matches(Object o)
      {
        if(!(o instanceof IllegalArgumentException))
        {
          return false;
        }
        return ((IllegalArgumentException) o).getMessage().startsWith("Command line can not be empty");
      }

      @Override
      public void describeTo(Description description)
      {
        description.appendText("IllegalArgumentException: Command line can not be empty");
      }
    });
    checkValues(new String[]{" "});
  }

  @Test
  public void testPatternMatcherRejectsUnbalancedQuoteJavaOptions()
  {
    expectedException.expect(new BaseMatcher<Object>()
    {
      @Override
      public boolean matches(Object o)
      {
        if(!(o instanceof IllegalArgumentException))
        {
          return false;
        }
        return ((IllegalArgumentException) o).getMessage().startsWith("Unbalanced quotes in");
      }

      @Override
      public void describeTo(Description description)
      {
        description.appendText("IllegalArgumentException: Unbalanced quotes in");
      }
    });
    CommandLine.parse("\"");
  }

  @Test
  public void testPatternMatcherPreservesNonBreakingSpacesJavaOptions()
  {
    // SUPER AWESOME VOODOO MAGIC. These are not normal spaces, they are non-breaking spaces.
    checkValues(new String[]{"keep me around"});
  }


  @Test
  public void testPatternMatcherForSimpleJavaOptions()
  {
    checkValues(
        new String[]{
            "test",
            "-mmm\"some quote with\"suffix",
            "test2",
            "\"completely quoted\"",
            "more",
            "☃",
            "-XX:SomeCoolOption=false",
            "-XX:SomeOption=\"with spaces\"",
            "someValues",
            "some\"strange looking\"option",
            "andOtherOptions",
            "\"\"",
            "AndMaybeEmptyQuotes"
        }
    );
    checkValues(new String[]{"\"completely quoted\""});
    checkValues(new String[]{"-foo=\"\""});
    checkValues(new String[]{"-foo=\"\"suffix"});
    checkValues(new String[]{"-foo=\"\t\"suffix"});
    checkValues(new String[]{"-foo=\"\t\r\n\f     \"suffix"});
    checkValues(new String[]{"-foo=\"\t\r\n\f     \""});
    checkValues(new String[]{"\"\t\r\n\f     \"suffix"});
    checkValues(new String[]{"-foo=\"\"suffix", "more"});
  }

  @Test
  public void testFarApart()
  {
    Assert.assertEquals(
        ImmutableList.of("start\t\t\t\t", "", "", "", "stop"), ImmutableList.copyOf(
            CommandLine.parse(
                "start\t\t\t\t \n\f\r\n \f\f \n\r\f\n\r\t stop"
            ).toStrings()
        )
    );
  }

  @Test
  public void testProblematicOption()
  {
    Assert.assertEquals(
        ImmutableList.of("java", "-XX:OnOutOfMemoryError=kill -9 %p"),
        ImmutableList.copyOf(CommandLine.parse("java -XX:OnOutOfMemoryError=\"kill -9 %p\"").toStrings())
    );
  }

  @Test
  public void testRejectEmpty()
  {
    expectedException.expect(new BaseMatcher<Object>()
    {
      @Override
      public boolean matches(Object o)
      {
        if(!(o instanceof IllegalArgumentException))
        {
          return false;
        }
        return ((IllegalArgumentException) o).getMessage().startsWith("Command line can not be empty");
      }

      @Override
      public void describeTo(Description description)
      {
        description.appendText("IllegalArgumentException: Command line can not be empty");
      }
    });
    CommandLine.parse(" \t     \t\t\t\t \n\n \f\f \n\f\r\t");
  }

  final static Function<String, String> QUOTE_STRIPPER = new Function<String, String>()
  {
    final static String QUOTE = "\"";

    @Override
    public String apply(String input)
    {
      return input.replace(QUOTE, "");
    }
  };

  final static Function<String, String> QUOTE_END_STRIPPER = new Function<String, String>()
  {
    final static String QUOTE = "\"";

    @Override
    public String apply(String input)
    {
      if(input.startsWith(QUOTE)) {
        if(input.endsWith(QUOTE)) {
          return input.substring(1, input.length() - 1);
        } else {
          throw new IAE("input [%s] should end with quote");
        }
      } else {
        return input;
      }
    }
  };

  private static void checkValues(String[] strings)
  {
    Assert.assertEquals(
        ImmutableList.copyOf(Lists.transform(ImmutableList.copyOf(strings), QUOTE_STRIPPER)),
        ImmutableList.copyOf(Iterables.transform(ImmutableList.copyOf(
            CommandLine.parse(Joiner.on(" ").join(strings)).toStrings()
        ), QUOTE_END_STRIPPER))
    );
  }
}
