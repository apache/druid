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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinMatcher;
import org.apache.druid.segment.join.Joinable;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class LookupJoinable implements Joinable
{
  static final List<String> ALL_COLUMNS = ImmutableList.of(
      LookupColumnSelectorFactory.KEY_COLUMN,
      LookupColumnSelectorFactory.VALUE_COLUMN
  );

  private final LookupExtractor extractor;

  private LookupJoinable(LookupExtractor extractor)
  {
    this.extractor = extractor;
  }

  public static LookupJoinable wrap(final LookupExtractor extractor)
  {
    return new LookupJoinable(extractor);
  }

  @Override
  public List<String> getAvailableColumns()
  {
    return ALL_COLUMNS;
  }

  @Override
  public int getCardinality(String columnName)
  {
    return CARDINALITY_UNKNOWN;
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    if (ALL_COLUMNS.contains(columnName)) {
      return new ColumnCapabilitiesImpl().setType(ColumnType.STRING);
    } else {
      return null;
    }
  }

  @Override
  public JoinMatcher makeJoinMatcher(
      final ColumnSelectorFactory leftSelectorFactory,
      final JoinConditionAnalysis condition,
      final boolean remainderNeeded,
      boolean descending,
      Closer closer
  )
  {
    return LookupJoinMatcher.create(extractor, leftSelectorFactory, condition, remainderNeeded);
  }

  @Override
  public ColumnValuesWithUniqueFlag getNonNullColumnValues(String columnName, int maxNumValues)
  {
    if (LookupColumnSelectorFactory.KEY_COLUMN.equals(columnName) && extractor.canGetKeySet()) {
      final Set<String> keys = extractor.keySet();

      final Set<String> nullEquivalentValues = new HashSet<>();
      nullEquivalentValues.add(null);
      if (NullHandling.replaceWithDefault()) {
        nullEquivalentValues.add(NullHandling.defaultStringValue());
      }

      // size() of Sets.difference is slow; avoid it.
      int nonNullKeys = keys.size();

      for (String value : nullEquivalentValues) {
        if (keys.contains(value)) {
          nonNullKeys--;
        }
      }

      if (nonNullKeys > maxNumValues) {
        return new ColumnValuesWithUniqueFlag(ImmutableSet.of(), false);
      } else if (nonNullKeys == keys.size()) {
        return new ColumnValuesWithUniqueFlag(keys, true);
      } else {
        return new ColumnValuesWithUniqueFlag(Sets.difference(keys, nullEquivalentValues), true);
      }
    } else {
      return new ColumnValuesWithUniqueFlag(ImmutableSet.of(), false);
    }
  }

  @Override
  public Optional<Set<String>> getCorrelatedColumnValues(
      String searchColumnName,
      String searchColumnValue,
      String retrievalColumnName,
      long maxCorrelationSetSize,
      boolean allowNonKeyColumnSearch
  )
  {
    if (!ALL_COLUMNS.contains(searchColumnName) || !ALL_COLUMNS.contains(retrievalColumnName)) {
      return Optional.empty();
    }
    Set<String> correlatedValues;
    if (LookupColumnSelectorFactory.KEY_COLUMN.equals(searchColumnName)) {
      if (LookupColumnSelectorFactory.KEY_COLUMN.equals(retrievalColumnName)) {
        correlatedValues = ImmutableSet.of(searchColumnValue);
      } else {
        // This should not happen in practice because the column to be joined on must be a key.
        correlatedValues = Collections.singleton(extractor.apply(searchColumnValue));
      }
    } else {
      if (!allowNonKeyColumnSearch) {
        return Optional.empty();
      }
      if (LookupColumnSelectorFactory.VALUE_COLUMN.equals(retrievalColumnName)) {
        // This should not happen in practice because the column to be joined on must be a key.
        correlatedValues = ImmutableSet.of(searchColumnValue);
      } else {
        // Lookup extractor unapply only provides a list of strings, so we can't respect
        // maxCorrelationSetSize easily. This should be handled eventually.
        correlatedValues = ImmutableSet.copyOf(extractor.unapply(searchColumnValue));
      }
    }
    return Optional.of(correlatedValues);
  }

  @Override
  public Optional<Closeable> acquireReferences()
  {
    // nothing to close for lookup joinables, they are managed externally and have no per query accounting of usage
    return Optional.of(() -> {});
  }
}
