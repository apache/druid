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

package org.apache.druid.query.topn.vector;

import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TopNVectorColumnProcessorFactoryTest
{
  private final TopNVectorColumnProcessorFactory factory = TopNVectorColumnProcessorFactory.instance();

  @Test
  public void testUseDictionaryEncodedSelectorTrueForUniqueDictEncodedString()
  {
    final ColumnCapabilitiesImpl caps = new ColumnCapabilitiesImpl()
        .setType(ColumnType.STRING)
        .setDictionaryEncoded(true)
        .setDictionaryValuesUnique(true)
        .setHasMultipleValues(false);

    Assertions.assertTrue(factory.useDictionaryEncodedSelector(caps));
  }

  @Test
  public void testUseDictionaryEncodedSelectorFalseForNonUniqueDict()
  {
    // Non-unique dict (two dict IDs can map to the same string) must route through the object path so the
    // DictionaryBuildingSingleValueStringTopNVectorColumnSelector collapses aliased IDs by string value.
    final ColumnCapabilitiesImpl caps = new ColumnCapabilitiesImpl()
        .setType(ColumnType.STRING)
        .setDictionaryEncoded(true)
        .setDictionaryValuesUnique(false)
        .setHasMultipleValues(false);

    Assertions.assertFalse(factory.useDictionaryEncodedSelector(caps));
  }

  @Test
  public void testUseDictionaryEncodedSelectorFalseForNonDictEncodedString()
  {
    final ColumnCapabilitiesImpl caps = new ColumnCapabilitiesImpl()
        .setType(ColumnType.STRING)
        .setDictionaryEncoded(false)
        .setDictionaryValuesUnique(false)
        .setHasMultipleValues(false);

    Assertions.assertFalse(factory.useDictionaryEncodedSelector(caps));
  }

  @Test
  public void testDictEncodedUniqueStringProducesDictIdSelector()
  {
    final ColumnCapabilitiesImpl caps = new ColumnCapabilitiesImpl()
        .setType(ColumnType.STRING)
        .setDictionaryEncoded(true)
        .setDictionaryValuesUnique(true)
        .setHasMultipleValues(false);
    final SingleValueDimensionVectorSelector selector = Mockito.mock(SingleValueDimensionVectorSelector.class);

    final TopNVectorColumnSelector processor = factory.makeSingleValueDimensionProcessor(caps, selector);

    Assertions.assertTrue(processor instanceof SingleValueStringTopNVectorColumnSelector);
  }

  @Test
  public void testNonUniqueDictRoutesObjectPathToDictionaryBuildingSelector()
  {
    // When a non-unique dict routes through makeObjectProcessor, it must produce the dictionary-building selector
    // so aliased dict IDs are collapsed by string value — matching the non-vectorized path's semantics.
    final ColumnCapabilitiesImpl caps = new ColumnCapabilitiesImpl()
        .setType(ColumnType.STRING)
        .setDictionaryEncoded(true)
        .setDictionaryValuesUnique(false)
        .setHasMultipleValues(false);
    final VectorObjectSelector selector = Mockito.mock(VectorObjectSelector.class);

    final TopNVectorColumnSelector processor = factory.makeObjectProcessor(caps, selector);

    Assertions.assertTrue(processor instanceof DictionaryBuildingSingleValueStringTopNVectorColumnSelector);
  }
}
