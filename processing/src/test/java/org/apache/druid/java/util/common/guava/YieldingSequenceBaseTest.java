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

package org.apache.druid.java.util.common.guava;

import com.google.common.collect.ImmutableList;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableCauseMatcher;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.util.ArrayList;

public class YieldingSequenceBaseTest
{
  @Test
  public void testAccumulate()
  {
    final ExplodingSequence<Integer> sequence = new ExplodingSequence<>(
        Sequences.simple(ImmutableList.of(1, 2, 3)),
        false,
        false
    );

    Assert.assertEquals(ImmutableList.of(1, 2, 3), sequence.accumulate(new ArrayList<>(), Accumulators.list()));
    Assert.assertEquals("Closes resources", 1, sequence.getCloseCount());
  }

  @Test
  public void testExceptionDuringGet()
  {
    final ExplodingSequence<Integer> sequence = new ExplodingSequence<>(
        Sequences.simple(ImmutableList.of(1, 2, 3)),
        true,
        false
    );

    try {
      sequence.accumulate(new ArrayList<>(), Accumulators.list());
      Assert.fail("Expected exception");
    }
    catch (Exception e) {
      Assert.assertThat(e, ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("get")));
    }

    Assert.assertEquals("Closes resources", 1, sequence.getCloseCount());
  }

  @Test
  public void testExceptionDuringClose()
  {
    final ExplodingSequence<Integer> sequence = new ExplodingSequence<>(
        Sequences.simple(ImmutableList.of(1, 2, 3)),
        false,
        true
    );

    try {
      sequence.accumulate(new ArrayList<>(), Accumulators.list());
      Assert.fail("Expected exception");
    }
    catch (Exception e) {
      Assert.assertThat(
          e,

          // Wrapped one level deep because it's an IOException
          ThrowableCauseMatcher.hasCause(ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("close")))
      );
    }

    Assert.assertEquals("Closes resources", 1, sequence.getCloseCount());
  }

  @Test
  public void testExceptionDuringGetAndClose()
  {
    final ExplodingSequence<Integer> sequence = new ExplodingSequence<>(
        Sequences.simple(ImmutableList.of(1, 2, 3)),
        true,
        true
    );

    try {
      sequence.accumulate(new ArrayList<>(), Accumulators.list());
      Assert.fail("Expected exception");
    }
    catch (Exception e) {
      Assert.assertThat(e, ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("get")));
    }

    Assert.assertEquals("Closes resources", 1, sequence.getCloseCount());
  }
}
