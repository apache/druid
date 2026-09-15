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

package org.apache.druid.segment;

import org.apache.druid.query.OrderBy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class EmptyCursorHolderTest
{
  private static final CursorBuildSpec ASC = CursorBuildSpec.builder()
                                                            .setPreferredOrdering(Cursors.ascendingTimeOrder())
                                                            .build();
  private static final CursorBuildSpec DESC = CursorBuildSpec.builder()
                                                             .setPreferredOrdering(Cursors.descendingTimeOrder())
                                                             .build();

  @Test
  void testForTimeOrderAscendingAdvertisesAscendingTime()
  {
    CursorHolder holder = EmptyCursorHolder.forSpec(ASC);
    Assertions.assertEquals(
        Cursors.ascendingTimeOrder(),
        holder.getOrdering()
    );
    Assertions.assertSame(EmptyCursorHolder.forSpec(ASC), holder);
  }

  @Test
  void testForTimeOrderDescendingAdvertisesDescendingTime()
  {
    CursorHolder holder = EmptyCursorHolder.forSpec(DESC);
    Assertions.assertEquals(
        Cursors.descendingTimeOrder(),
        holder.getOrdering()
    );
    Assertions.assertSame(EmptyCursorHolder.forSpec(DESC), holder);
  }

  @Test
  void testForTimeOrderNoneReturnsUnorderedInstance()
  {
    CursorHolder holder = EmptyCursorHolder.forSpec(CursorBuildSpec.FULL_SCAN);
    Assertions.assertEquals(
        List.of(),
        holder.getOrdering()
    );
    Assertions.assertSame(EmptyCursorHolder.forSpec(CursorBuildSpec.FULL_SCAN), holder);
  }

  @Test
  void testForSpecNonTimeOrderingAdvertisedVerbatim()
  {
    // preferredOrdering decouples from assumed __time ordering; an empty cursor trivially satisfies any ordering, so
    // forSpec advertises a non-__time preferred ordering verbatim via a fresh, uncached instance.
    final List<OrderBy> ordering = List.of(OrderBy.ascending("dim"));
    final CursorBuildSpec spec = CursorBuildSpec.builder().setPreferredOrdering(ordering).build();
    final CursorHolder holder = EmptyCursorHolder.forSpec(spec);
    Assertions.assertEquals(ordering, holder.getOrdering());
    // Not the unordered singleton, and arbitrary orderings are not cached (a fresh instance each call).
    Assertions.assertNotSame(holder, EmptyCursorHolder.forSpec(CursorBuildSpec.FULL_SCAN));
    Assertions.assertNotSame(holder, EmptyCursorHolder.forSpec(spec));
  }

  @Test
  void testCursorIsAlwaysDone()
  {
    Assertions.assertTrue(EmptyCursorHolder.forSpec(ASC).asCursor().isDone());
    Assertions.assertTrue(EmptyCursorHolder.forSpec(DESC).asCursor().isDone());
    Assertions.assertTrue(EmptyCursorHolder.forSpec(CursorBuildSpec.FULL_SCAN).asCursor().isDone());
  }
}
