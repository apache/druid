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

package org.apache.druid.segment.join.lookup;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.LookupDimensionSpec;
import org.apache.druid.query.dimension.RegexFilteredDimensionSpec;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.RetainedLookupExtractor;
import org.apache.druid.query.lookup.RetainingLookupExtractorFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinMatcher;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LookupColumnSelectorFactoryTest extends InitializedNullHandlingTest
{
  @ParameterizedTest
  @CsvSource({
      "k, false, old-key", "v, false, old-value", "absent, false, missing",
      "k, true, old-key", "v, true, old-value", "absent, true, missing"
  })
  public void testRetainsOneSnapshotUntilCursorClose(
      final String column,
      final boolean filtered,
      final String expected
  ) throws IOException
  {
    final AtomicInteger acquisitions = new AtomicInteger();
    final AtomicInteger releases = new AtomicInteger();
    final AtomicReference<Map<String, String>> values = new AtomicReference<>(
        ImmutableMap.of("key", "old-key", "value", "old-value")
    );
    final RetainingLookupExtractorFactory lookupFactory = new RetainingLookupExtractorFactory(
        () -> new MapLookupExtractor(values.get(), false),
        () -> {
          acquisitions.incrementAndGet();
          return Optional.of(RetainedLookupExtractor.create(
              new MapLookupExtractor(values.get(), false),
              releases::incrementAndGet
          ));
        }
    );
    final LookupExtractorFactoryContainerProvider provider =
        Mockito.mock(LookupExtractorFactoryContainerProvider.class);
    Mockito.when(provider.get("lookup"))
           .thenReturn(Optional.of(new LookupExtractorFactoryContainer("v0", lookupFactory)));
    final LookupDimensionSpec lookupSpec = new LookupDimensionSpec(
        column,
        column,
        null,
        false,
        "missing",
        "lookup",
        false,
        provider
    );
    final DimensionSpec dimensionSpec = filtered ? new RegexFilteredDimensionSpec(lookupSpec, ".*") : lookupSpec;
    final LookupJoinable joinable = LookupJoinable.wrap(new MapLookupExtractor(ImmutableMap.of("key", "value"), false));

    try (final Closer closer = Closer.create()) {
      final JoinMatcher matcher = joinable.makeJoinMatcher(
          Mockito.mock(ColumnSelectorFactory.class),
          JoinConditionAnalysis.forExpression("1", "j.", ExprMacroTable.nil()),
          false,
          closer
      );
      matcher.matchCondition();
      final DimensionSelector selector = matcher.getColumnSelectorFactory().makeDimensionSelector(dimensionSpec);
      Assertions.assertEquals(expected, selector.lookupName(selector.getRow().get(0)));
      values.set(ImmutableMap.of("key", "new-key", "value", "new-value"));
      for (int i = 0; i < 10; i++) {
        Assertions.assertEquals(expected, selector.lookupName(selector.getRow().get(0)));
      }
      Assertions.assertEquals(1, acquisitions.get());
      Assertions.assertEquals(0, releases.get());
    }
    Assertions.assertEquals(1, releases.get());
  }
}
