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

package org.apache.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;

@RunWith(Parameterized.class)
public class ExpressionFilterNonStrictBooleansTest extends ExpressionFilterTest
{
  public ExpressionFilterNonStrictBooleansTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    super(testName, indexBuilder, finisher, cnf, optimize);
  }

  @Before
  @Override
  public void setup()
  {
    ExpressionProcessing.initializeForStrictBooleansTests(false);
  }

  @Override
  @Test
  public void testComplement()
  {
    if (NullHandling.sqlCompatible()) {
      assertFilterMatches(edf("dim5 == 'a'"), ImmutableList.of("0"));
      // non-strict mode is wild
      assertFilterMatches(
          NotDimFilter.of(edf("dim5 == 'a'")),
          ImmutableList.of("1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
      assertFilterMatches(
          edf("dim5 == ''"), ImmutableList.of("4")
      );
      // non-strict mode!
      assertFilterMatches(
          NotDimFilter.of(edf("dim5 == ''")), ImmutableList.of("0", "1", "2", "3", "5", "6", "7", "8", "9")
      );
    } else {
      assertFilterMatches(edf("dim5 == 'a'"), ImmutableList.of("0"));
      assertFilterMatches(
          NotDimFilter.of(edf("dim5 == 'a'")),
          ImmutableList.of("1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
    }
  }

  @Override
  @Test
  public void testMissingColumn()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          edf("missing == ''"),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
      assertFilterMatches(
          edf("missing == otherMissing"),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
    } else {
      // AS per SQL standard null == null returns false.
      assertFilterMatches(edf("missing == null"), ImmutableList.of());
      // in non-strict mode, madness happens
      assertFilterMatches(
          NotDimFilter.of(edf("missing == null")),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
      // also this madness doesn't do madness
      assertFilterMatches(
          edf("missing == otherMissing"),
          ImmutableList.of()
      );
      assertFilterMatches(
          NotDimFilter.of(edf("missing == otherMissing")),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
    }
    assertFilterMatches(edf("missing == '1'"), ImmutableList.of());
    assertFilterMatches(edf("missing == 2"), ImmutableList.of());
    if (NullHandling.replaceWithDefault()) {
      // missing equivaluent to 0
      assertFilterMatches(
          edf("missing < '2'"),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
      assertFilterMatches(
          edf("missing < 2"),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
      assertFilterMatches(
          edf("missing < 2.0"),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
    } else {
      // missing equivalent to null
      assertFilterMatches(edf("missing < '2'"), ImmutableList.of());
      assertFilterMatches(edf("missing < 2"), ImmutableList.of());
      assertFilterMatches(edf("missing < 2.0"), ImmutableList.of());
    }
    assertFilterMatches(edf("missing > '2'"), ImmutableList.of());
    assertFilterMatches(edf("missing > 2"), ImmutableList.of());
    assertFilterMatches(edf("missing > 2.0"), ImmutableList.of());
    assertFilterMatchesSkipVectorize(edf("like(missing, '1%')"), ImmutableList.of());
  }
}
