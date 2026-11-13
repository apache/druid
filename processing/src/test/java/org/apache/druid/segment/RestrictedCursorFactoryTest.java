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

import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class RestrictedCursorFactoryTest extends InitializedNullHandlingTest
{
  private static final Closer CLOSER = Closer.create();
  private static CursorFactory base;

  @BeforeAll
  public static void setUpClass(@TempDir final File tempDir)
  {
    base = new QueryableIndexCursorFactory(CLOSER.register(SmallSegmentTestHelper.makeIndex(tempDir)));
  }

  @AfterAll
  public static void tearDownClass() throws IOException
  {
    CLOSER.close();
  }

  @Test
  public void test_rowFilterPolicy_filtersRows()
  {
    final RestrictedCursorFactory restricted = new RestrictedCursorFactory(
        base,
        RowFilterPolicy.from(new EqualityFilter("dim", ColumnType.STRING, "b", null))
    );
    Assertions.assertEquals(
        List.of(List.of("b")),
        SmallSegmentTestHelper.readRows(restricted, CursorBuildSpec.FULL_SCAN, "dim")
    );
  }

  @Test
  public void test_prefetch_delegatesSameSpecAsAsyncBuild()
  {
    final DeferredCursorFactory deferred = new DeferredCursorFactory(base);
    final RestrictedCursorFactory restricted = new RestrictedCursorFactory(
        deferred,
        RowFilterPolicy.from(new EqualityFilter("dim", ColumnType.STRING, "b", null))
    );

    restricted.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN).close();
    final CursorPrefetch prefetch = restricted.prefetch(CursorBuildSpec.FULL_SCAN);
    try {
      Assertions.assertEquals(deferred.getPendingSpec(), deferred.getPrefetchSpec());
      Assertions.assertNotNull(deferred.getPrefetchSpec().getFilter(), "the policy's row filter is applied");
      Assertions.assertFalse(prefetch.isReady());
      deferred.completePrefetch();
      Assertions.assertTrue(prefetch.isReady());
    }
    finally {
      prefetch.close();
    }
  }

  @Test
  public void test_prefetch_noRestrictionPassesSpecThrough()
  {
    final DeferredCursorFactory deferred = new DeferredCursorFactory(base);
    final RestrictedCursorFactory restricted = new RestrictedCursorFactory(deferred, NoRestrictionPolicy.instance());

    final CursorPrefetch prefetch = restricted.prefetch(CursorBuildSpec.FULL_SCAN);
    Assertions.assertEquals(CursorBuildSpec.FULL_SCAN, deferred.getPrefetchSpec());
    prefetch.close();
    Assertions.assertTrue(deferred.prefetchCanceled.get(), "closing before ready cancels the delegate's prefetch");
  }
}
