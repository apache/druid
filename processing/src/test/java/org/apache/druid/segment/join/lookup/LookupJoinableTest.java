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
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.join.Joinable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class LookupJoinableTest
{
  private static final String UNKNOWN_COLUMN = "UNKNOWN_COLUMN";
  private static final String SEARCH_KEY_VALUE = "SEARCH_KEY_VALUE";
  private static final String SEARCH_KEY_NULL_VALUE = "SEARCH_KEY_NULL_VALUE";
  private static final String SEARCH_VALUE_VALUE = "SEARCH_VALUE_VALUE";
  private static final String SEARCH_VALUE_UNKNOWN = "SEARCH_VALUE_UNKNOWN";

  @Mock
  private LookupExtractor extractor;

  private LookupJoinable target;

  @Before
  public void setUp()
  {
    Mockito.doReturn(SEARCH_VALUE_VALUE).when(extractor).apply(SEARCH_KEY_VALUE);
    Mockito.doReturn(ImmutableList.of(SEARCH_KEY_VALUE)).when(extractor).unapply(SEARCH_VALUE_VALUE);
    Mockito.doReturn(ImmutableList.of()).when(extractor).unapply(SEARCH_VALUE_UNKNOWN);
    target = LookupJoinable.wrap(extractor);
  }

  @Test
  public void getAvailableColumnShouldReturnOnlyTwoColumns()
  {
    List<String> colummns = target.getAvailableColumns();
    Assert.assertEquals(2, colummns.size());
    Assert.assertEquals(
        ImmutableList.of(LookupColumnSelectorFactory.KEY_COLUMN, LookupColumnSelectorFactory.VALUE_COLUMN),
        colummns
    );
  }

  @Test
  public void getCardinalityForUnknownColumnShouldReturnUnknown()
  {
    int cardinality = target.getCardinality(UNKNOWN_COLUMN);
    Assert.assertEquals(Joinable.CARDINALITY_UNKNOWN, cardinality);
  }

  @Test
  public void getCardinalityForKeyColumnShouldReturnUnknown()
  {
    int cardinality = target.getCardinality(LookupColumnSelectorFactory.KEY_COLUMN);
    Assert.assertEquals(Joinable.CARDINALITY_UNKNOWN, cardinality);
  }

  @Test
  public void getCardinalityForValueColumnShouldReturnUnknown()
  {
    int cardinality = target.getCardinality(LookupColumnSelectorFactory.VALUE_COLUMN);
    Assert.assertEquals(Joinable.CARDINALITY_UNKNOWN, cardinality);
  }

  @Test
  public void getColumnCapabilitiesForKeyColumnShouldReturnStringCaps()
  {
    ColumnCapabilities capabilities = target.getColumnCapabilities(LookupColumnSelectorFactory.KEY_COLUMN);
    Assert.assertEquals(ValueType.STRING, capabilities.getType());
  }

  @Test
  public void getColumnCapabilitiesForValueColumnShouldReturnStringCaps()
  {
    ColumnCapabilities capabilities = target.getColumnCapabilities(LookupColumnSelectorFactory.VALUE_COLUMN);
    Assert.assertEquals(ValueType.STRING, capabilities.getType());
  }

  @Test
  public void getColumnCapabilitiesForUnknownColumnShouldReturnNull()
  {
    ColumnCapabilities capabilities = target.getColumnCapabilities(UNKNOWN_COLUMN);
    Assert.assertNull(capabilities);
  }

  @Test
  public void getCorrelatedColummnValuesMissingSearchColumnShouldReturnEmptySet()
  {
    Optional<Set<String>> correlatedValues =
        target.getCorrelatedColumnValues(
            UNKNOWN_COLUMN,
            SEARCH_KEY_VALUE,
            LookupColumnSelectorFactory.VALUE_COLUMN,
            0,
            false);

    Assert.assertFalse(correlatedValues.isPresent());
  }

  @Test
  public void getCorrelatedColummnValuesMissingRetrievalColumnShouldReturnEmptySet()
  {
    Optional<Set<String>> correlatedValues =
        target.getCorrelatedColumnValues(
            LookupColumnSelectorFactory.KEY_COLUMN,
            SEARCH_KEY_VALUE,
            UNKNOWN_COLUMN,
            0,
            false);

    Assert.assertFalse(correlatedValues.isPresent());
  }
  @Test
  public void getCorrelatedColumnValuesForSearchKeyAndRetrieveKeyColumnShouldReturnSearchValue()
  {
    Optional<Set<String>> correlatedValues = target.getCorrelatedColumnValues(
        LookupColumnSelectorFactory.KEY_COLUMN,
        SEARCH_KEY_VALUE,
        LookupColumnSelectorFactory.KEY_COLUMN,
        0,
        false);
    Assert.assertEquals(Optional.of(ImmutableSet.of(SEARCH_KEY_VALUE)), correlatedValues);
  }

  @Test
  public void getCorrelatedColumnValuesForSearchKeyAndRetrieveValueColumnShouldReturnExtractedValue()
  {
    Optional<Set<String>> correlatedValues = target.getCorrelatedColumnValues(
        LookupColumnSelectorFactory.KEY_COLUMN,
        SEARCH_KEY_VALUE,
        LookupColumnSelectorFactory.VALUE_COLUMN,
        0,
        false);
    Assert.assertEquals(Optional.of(ImmutableSet.of(SEARCH_VALUE_VALUE)), correlatedValues);
  }

  @Test
  public void getCorrelatedColumnValuesForSearchKeyMissingAndRetrieveValueColumnShouldReturnExtractedValue()
  {
    Optional<Set<String>> correlatedValues = target.getCorrelatedColumnValues(
        LookupColumnSelectorFactory.KEY_COLUMN,
        SEARCH_KEY_NULL_VALUE,
        LookupColumnSelectorFactory.VALUE_COLUMN,
        0,
        false);
    Assert.assertEquals(Optional.of(Collections.singleton(null)), correlatedValues);
  }

  @Test
  public void getCorrelatedColumnValuesForSearchValueAndRetrieveValueColumnAndNonKeyColumnSearchDisabledShouldReturnSearchValue()
  {
    Optional<Set<String>> correlatedValues = target.getCorrelatedColumnValues(
        LookupColumnSelectorFactory.VALUE_COLUMN,
        SEARCH_VALUE_VALUE,
        LookupColumnSelectorFactory.VALUE_COLUMN,
        10,
        false);
    Assert.assertEquals(Optional.empty(), correlatedValues);
    correlatedValues = target.getCorrelatedColumnValues(
        LookupColumnSelectorFactory.VALUE_COLUMN,
        SEARCH_VALUE_VALUE,
        LookupColumnSelectorFactory.KEY_COLUMN,
        10,
        false);
    Assert.assertEquals(Optional.empty(), correlatedValues);
  }

  @Test
  public void getCorrelatedColumnValuesForSearchValueAndRetrieveValueColumnShouldReturnSearchValue()
  {
    Optional<Set<String>> correlatedValues = target.getCorrelatedColumnValues(
        LookupColumnSelectorFactory.VALUE_COLUMN,
        SEARCH_VALUE_VALUE,
        LookupColumnSelectorFactory.VALUE_COLUMN,
        0,
        true);
    Assert.assertEquals(Optional.of(ImmutableSet.of(SEARCH_VALUE_VALUE)), correlatedValues);
  }

  @Test
  public void getCorrelatedColumnValuesForSearchValueAndRetrieveKeyColumnShouldReturnUnAppliedValue()
  {
    Optional<Set<String>> correlatedValues = target.getCorrelatedColumnValues(
        LookupColumnSelectorFactory.VALUE_COLUMN,
        SEARCH_VALUE_VALUE,
        LookupColumnSelectorFactory.KEY_COLUMN,
        10,
        true);
    Assert.assertEquals(Optional.of(ImmutableSet.of(SEARCH_KEY_VALUE)), correlatedValues);
  }

  @Test
  @Ignore
  /**
   * See {@link LookupJoinable#getCorrelatedColumnValues(String, String, String, long, boolean)} for implementation
   * details that cause this test to fail.
   */
  public void getCorrelatedColumnValuesForSearchValueAndRetrieveKeyColumnWithMaxLimitSetShouldHonorMaxLimit()
  {
    Optional<Set<String>> correlatedValues = target.getCorrelatedColumnValues(
        LookupColumnSelectorFactory.VALUE_COLUMN,
        SEARCH_VALUE_VALUE,
        LookupColumnSelectorFactory.KEY_COLUMN,
        0,
        true);
    Assert.assertEquals(Optional.empty(), correlatedValues);
  }

  @Test
  public void getCorrelatedColumnValuesForSearchUnknownValueAndRetrieveKeyColumnShouldReturnNoCorrelatedValues()
  {
    Optional<Set<String>> correlatedValues = target.getCorrelatedColumnValues(
        LookupColumnSelectorFactory.VALUE_COLUMN,
        SEARCH_VALUE_UNKNOWN,
        LookupColumnSelectorFactory.KEY_COLUMN,
        10,
        true);
    Assert.assertEquals(Optional.of(ImmutableSet.of()), correlatedValues);
  }
}
