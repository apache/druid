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

package org.apache.druid.query.scan;

import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.guava.Sequences;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ScanQueryOffsetSequenceTest
{
  private final List<List<Integer>> rowList1 = ImmutableList.of(
      ImmutableList.of(1, 2),
      ImmutableList.of(3, 4),
      ImmutableList.of(5, 6)
  );

  private final List<List<Integer>> rowList2 = ImmutableList.of(
      ImmutableList.of(7, 8),
      ImmutableList.of(9, 10),
      ImmutableList.of(11, 12)
  );

  @Test
  public void testSkip()
  {
    final List<ScanResultValue> unskipped = makeExpectedResults(0);

    for (int skip = 1; skip <= rowList1.size() + rowList2.size() + 1; skip++) {
      final List<ScanResultValue> expected = makeExpectedResults(skip);
      final List<ScanResultValue> resultsAfterSkip = new ScanQueryOffsetSequence(
          Sequences.simple(unskipped),
          skip
      ).toList();

      Assert.assertEquals("skip = " + skip, expected, resultsAfterSkip);
    }
  }

  /**
   * Return a list of 0, 1, or 2 {@link ScanResultValue} based on the "skip" parameter. Rows are taken from
   * {@link #rowList1} and {@link #rowList2}.
   */
  private List<ScanResultValue> makeExpectedResults(final int skip)
  {
    final List<ScanResultValue> expected;

    if (skip < rowList1.size()) {
      // Somewhere inside the first ScanResultValue.
      expected = ImmutableList.of(
          new ScanResultValue(
              "1",
              ImmutableList.of("a", "b"),
              rowList1.subList(skip, rowList1.size())
          ),
          new ScanResultValue(
              "2",
              ImmutableList.of("b", "c"),
              rowList2
          )
      );
    } else if (skip < rowList1.size() + rowList2.size()) {
      // Somewhere inside the second ScanResultValue.
      expected = ImmutableList.of(
          new ScanResultValue(
              "2",
              ImmutableList.of("b", "c"),
              rowList2.subList(skip - rowList1.size(), rowList2.size())
          )
      );
    } else {
      // Past the second ScanResultValue.
      expected = ImmutableList.of();
    }

    return expected;
  }
}
