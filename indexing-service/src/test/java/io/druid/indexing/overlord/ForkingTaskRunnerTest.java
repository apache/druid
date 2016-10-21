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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;

public class ForkingTaskRunnerTest
{
  // This tests the test to make sure the test fails when it should.
  @Test(expected = AssertionError.class)
  public void testPatternMatcherFailureForJavaOptions()
  {
    checkValues(new String[]{"not quoted has space"});
  }

  @Test(expected = AssertionError.class)
  public void testPatternMatcherFailureForSpaceOnlyJavaOptions()
  {
    checkValues(new String[]{" "});
  }

  @Test
  public void testPatternMatcherLeavesUnbalancedQuoteJavaOptions()
  {
    Assert.assertEquals("\"", Iterators.get(new QuotableWhiteSpaceSplitter("\"").iterator(), 0));
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
            "AndMaybeEmptyQuotes",
            "keep me around"
        }
    );
    checkValues(new String[]{"\"completely quoted\""});
    checkValues(new String[]{"\"\""});
    checkValues(new String[]{"-foo=\"\""});
    checkValues(new String[]{"-foo=\"\"suffix"});
    checkValues(new String[]{"-foo=\"\t\"suffix"});
    checkValues(new String[]{"-foo=\"\t\r\n\f     \"suffix"});
    checkValues(new String[]{"-foo=\"\t\r\n\f     \""});
    checkValues(new String[]{"\"\t\r\n\f     \"suffix"});
    checkValues(new String[]{"-foo=\"\"suffix", "more"});
  }

  @Test
  public void testEmpty()
  {
    Assert.assertTrue(ImmutableList.copyOf(new QuotableWhiteSpaceSplitter("")).isEmpty());
  }

  @Test
  public void testFarApart()
  {
    Assert.assertEquals(
        ImmutableList.of("start", "stop"), ImmutableList.copyOf(
            new QuotableWhiteSpaceSplitter(
                "start\t\t\t\t \n\f\r\n \f\f \n\r\f\n\r\t stop"
            )
        )
    );
  }

  @Test
  public void testOmitEmpty()
  {
    Assert.assertTrue(
        ImmutableList.copyOf(
            new QuotableWhiteSpaceSplitter(" \t     \t\t\t\t \n\n \f\f \n\f\r\t")
        ).isEmpty()
    );
  }

  private static void checkValues(String[] strings)
  {
    Assert.assertEquals(
        ImmutableList.copyOf(strings),
        ImmutableList.copyOf(new QuotableWhiteSpaceSplitter(Joiner.on(" ").join(strings)))
    );
  }
}
