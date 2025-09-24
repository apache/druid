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

package org.apache.druid.indexing.seekablestream.common;

import org.junit.Assert;
import org.junit.Test;

import javax.validation.constraints.NotNull;

public class OrderedSequenceNumberTest
{
  @Test
  public void test_isMoreToReadBeforeReadingRecord_exclusiveEnd_lessThan()
  {
    TestSequenceNumber current = new TestSequenceNumber(5L, false);
    TestSequenceNumber end = new TestSequenceNumber(10L, false);

    Assert.assertTrue("Should have more to read when current < end with exclusive end",
                     current.isMoreToReadBeforeReadingRecord(end, true));
  }

  @Test
  public void test_isMoreToReadBeforeReadingRecord_exclusiveEnd_equalTo()
  {
    TestSequenceNumber current = new TestSequenceNumber(10L, false);
    TestSequenceNumber end = new TestSequenceNumber(10L, false);

    Assert.assertFalse("Should NOT have more to read when current == end with exclusive end",
                      current.isMoreToReadBeforeReadingRecord(end, true));
  }

  @Test
  public void testIsMoreToReadBeforeReadingRecord_exclusiveEnd_greaterThan()
  {
    TestSequenceNumber current = new TestSequenceNumber(15L, false);
    TestSequenceNumber end = new TestSequenceNumber(10L, false);

    Assert.assertFalse("Should NOT have more to read when current > end with exclusive end",
                      current.isMoreToReadBeforeReadingRecord(end, true));
  }

  @Test
  public void testIsMoreToReadBeforeReadingRecord_inclusiveEnd_lessThan()
  {
    TestSequenceNumber current = new TestSequenceNumber(5L, false);
    TestSequenceNumber end = new TestSequenceNumber(10L, false);

    Assert.assertTrue("Should have more to read when current < end with inclusive end",
                     current.isMoreToReadBeforeReadingRecord(end, false));
  }

  @Test
  public void testIsMoreToReadBeforeReadingRecord_inclusiveEnd_equalTo()
  {
    TestSequenceNumber current = new TestSequenceNumber(10L, false);
    TestSequenceNumber end = new TestSequenceNumber(10L, false);

    Assert.assertTrue("Should have more to read when current == end with inclusive end",
                     current.isMoreToReadBeforeReadingRecord(end, false));
  }

  @Test
  public void testIsMoreToReadBeforeReadingRecord_inclusiveEnd_greaterThan()
  {
    TestSequenceNumber current = new TestSequenceNumber(15L, false);
    TestSequenceNumber end = new TestSequenceNumber(10L, false);

    Assert.assertFalse("Should NOT have more to read when current > end with inclusive end",
                      current.isMoreToReadBeforeReadingRecord(end, false));
  }

  @Test
  public void testIsMoreToReadBeforeReadingRecord_nullEndSequenceNumber_exclusiveEnd()
  {
    TestSequenceNumber current = new TestSequenceNumber(5L, false);
    TestSequenceNumber end = new TestSequenceNumber(null, false);

    Assert.assertFalse("Should return false when end sequence number is null",
                      current.isMoreToReadBeforeReadingRecord(end, true));
  }

  @Test
  public void testIsMoreToReadBeforeReadingRecord_nullEndSequenceNumber_inclusiveEnd()
  {
    TestSequenceNumber current = new TestSequenceNumber(5L, false);
    TestSequenceNumber end = new TestSequenceNumber(null, false);

    Assert.assertFalse("Should return false when end sequence number is null",
                      current.isMoreToReadBeforeReadingRecord(end, false));
  }

  @Test
  public void testIsMoreToReadBeforeReadingRecord_nullCurrentSequenceNumber_exclusiveEnd()
  {
    TestSequenceNumber current = new TestSequenceNumber(null, false);
    TestSequenceNumber end = new TestSequenceNumber(10L, false);

    Assert.assertThrows(NullPointerException.class, () -> current.isMoreToReadBeforeReadingRecord(end, true));
  }

  @Test
  public void testIsMoreToReadBeforeReadingRecord_nullCurrentSequenceNumber_inclusiveEnd()
  {
    TestSequenceNumber current = new TestSequenceNumber(null, false);
    TestSequenceNumber end = new TestSequenceNumber(10L, false);

    Assert.assertThrows(NullPointerException.class, () -> current.isMoreToReadBeforeReadingRecord(end, false));
  }

  @Test
  public void testIsMoreToReadBeforeReadingRecord_bothNull()
  {
    TestSequenceNumber current = new TestSequenceNumber(null, false);
    TestSequenceNumber end = new TestSequenceNumber(null, false);

    Assert.assertFalse("Should return false when both sequence numbers are null",
                      current.isMoreToReadBeforeReadingRecord(end, true));
    Assert.assertFalse("Should return false when both sequence numbers are null",
                      current.isMoreToReadBeforeReadingRecord(end, false));
  }

  @Test
  public void testIsMoreToReadBeforeReadingRecord_exceptionHandling()
  {
    TestExceptionSequenceNumber current = new TestExceptionSequenceNumber(5L, false);
    TestExceptionSequenceNumber end = new TestExceptionSequenceNumber(10L, false);

    Assert.assertThrows(RuntimeException.class, () -> current.isMoreToReadBeforeReadingRecord(end, true));
    Assert.assertThrows(RuntimeException.class, () -> current.isMoreToReadBeforeReadingRecord(end, false));
  }

  @Test
  public void testIsMoreToReadBeforeReadingRecord_differentExclusivityFlags()
  {
    TestSequenceNumber currentExclusive = new TestSequenceNumber(5L, true);
    TestSequenceNumber currentInclusive = new TestSequenceNumber(5L, false);
    TestSequenceNumber endExclusive = new TestSequenceNumber(10L, true);
    TestSequenceNumber endInclusive = new TestSequenceNumber(10L, false);

    // Test different combinations with exclusive end
    Assert.assertTrue("Should work with different exclusivity flags",
                     currentExclusive.isMoreToReadBeforeReadingRecord(endExclusive, true));
    Assert.assertTrue("Should work with different exclusivity flags",
                     currentExclusive.isMoreToReadBeforeReadingRecord(endInclusive, true));
    Assert.assertTrue("Should work with different exclusivity flags",
                     currentInclusive.isMoreToReadBeforeReadingRecord(endExclusive, true));

    // Test different combinations with inclusive end
    Assert.assertTrue("Should work with different exclusivity flags",
                     currentExclusive.isMoreToReadBeforeReadingRecord(endExclusive, false));
    Assert.assertTrue("Should work with different exclusivity flags",
                     currentExclusive.isMoreToReadBeforeReadingRecord(endInclusive, false));
    Assert.assertTrue("Should work with different exclusivity flags",
                     currentInclusive.isMoreToReadBeforeReadingRecord(endExclusive, false));
  }

  /**
   * Test implementation of OrderedSequenceNumber for Long values
   */
  private static class TestSequenceNumber extends OrderedSequenceNumber<Long>
  {
    public TestSequenceNumber(Long sequenceNumber, boolean isExclusive)
    {
      super(sequenceNumber, isExclusive);
    }

    @Override
    public int compareTo(@NotNull OrderedSequenceNumber<Long> o)
    {
      return this.get().compareTo(o.get());
    }
  }

  /**
   * Test implementation that throws exceptions on comparison
   */
  private static class TestExceptionSequenceNumber extends OrderedSequenceNumber<Long>
  {
    public TestExceptionSequenceNumber(Long sequenceNumber, boolean isExclusive)
    {
      super(sequenceNumber, isExclusive);
    }

    @Override
    public int compareTo(@NotNull OrderedSequenceNumber<Long> o)
    {
      throw new RuntimeException("Comparison not supported");
    }
  }
}
