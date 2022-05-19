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

package org.apache.druid.query.dimension;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.function.Supplier;

public class ListFilteredDimensionSpecDimensionSelectorTest extends InitializedNullHandlingTest
{
  private final ListFilteredDimensionSpec targetWithAllowList = new ListFilteredDimensionSpec(
      new DefaultDimensionSpec("foo", "bar"),
      ImmutableSet.of("val1_1", "val2_2", "val2_3"),
      true
  );
  private final ListFilteredDimensionSpec targetWithDenyList = new ListFilteredDimensionSpec(
      new DefaultDimensionSpec("foo", "bar"),
      ImmutableSet.of("val1_1", "val2_2", "val2_3"),
      false
  );
  private final List<List<String>> data = ImmutableList.of(
      ImmutableList.of("val1_1", "val1_2"),
      ImmutableList.of("val2_1", "val2_2", "val2_3"),
      ImmutableList.of("val3_1")
  );

  @Test
  public void testNullDimensionSelectorReturnNull()
  {
    Assert.assertNull(targetWithAllowList.decorate((DimensionSelector) null));
    Assert.assertNull(targetWithDenyList.decorate((DimensionSelector) null));
  }

  @Test
  public void testAllowListWhenDictionaryLookupIsAvailable()
  {
    testAllowList(false, true, true, ForwardingFilteredDimensionSelector.class);
  }

  @Test
  public void testAllowListWhenIdLookupIsNotAvailable()
  {
    testAllowList(false, false, true, ForwardingFilteredDimensionSelector.class);
  }

  @Test
  public void testAllowListWhenCardinalityIsUnknown()
  {
    testAllowList(true, true, true, PredicateFilteredDimensionSelector.class);
  }

  @Test
  public void testAllowListWhenNameLookupIsNotPossibleInAdvance()
  {
    testAllowList(false, true, false, PredicateFilteredDimensionSelector.class);
  }

  @Test
  public void testDenyListWhenDictionaryLookupIsAvailable()
  {
    testDenyList(false, true, true, ForwardingFilteredDimensionSelector.class);
  }

  @Test
  public void testDenyListWhenIdLookupIsNotAvailable()
  {
    testDenyList(false, false, true, ForwardingFilteredDimensionSelector.class);
  }

  @Test
  public void testDenyListWhenCardinalityIsUnknown()
  {
    testDenyList(true, true, true, PredicateFilteredDimensionSelector.class);
  }

  @Test
  public void testDenyListWhenNameLookupIsNotPossibleInAdvance()
  {
    testDenyList(false, true, false, PredicateFilteredDimensionSelector.class);
  }

  private void testAllowList(
      boolean unknownCardinality,
      boolean validIdLookup,
      boolean nameLookupPossibleInAdvance,
      Class<? extends DimensionSelector> expectedDimensionSelectorClass
  )
  {
    RowSupplier rowSupplier = new RowSupplier();
    NonnullPair<Object2IntMap<String>, Int2ObjectMap<String>> dictionaries = createDictionaries(data);
    DimensionSelector selector = targetWithAllowList.decorate(
        new StringDimensionSelectorForTest(
            rowSupplier,
            dictionaries.lhs,
            dictionaries.rhs,
            unknownCardinality,
            validIdLookup,
            nameLookupPossibleInAdvance
        )
    );
    Assert.assertSame(expectedDimensionSelectorClass, selector.getClass());
    assertAllowListFiltering(rowSupplier, selector);
  }

  private void testDenyList(
      boolean unknownCardinality,
      boolean validIdLookup,
      boolean nameLookupPossibleInAdvance,
      Class<? extends DimensionSelector> expectedDimensionSelectorClass
  )
  {
    RowSupplier rowSupplier = new RowSupplier();
    NonnullPair<Object2IntMap<String>, Int2ObjectMap<String>> dictionaries = createDictionaries(data);
    DimensionSelector selector = targetWithDenyList.decorate(
        new StringDimensionSelectorForTest(
            rowSupplier,
            dictionaries.lhs,
            dictionaries.rhs,
            unknownCardinality,
            validIdLookup,
            nameLookupPossibleInAdvance
        )
    );
    Assert.assertSame(expectedDimensionSelectorClass, selector.getClass());

    assertDenyListFiltering(rowSupplier, selector);
  }

  private NonnullPair<Object2IntMap<String>, Int2ObjectMap<String>> createDictionaries(List<List<String>> values)
  {
    Object2IntMap<String> dictionary = new Object2IntOpenHashMap<>();
    Int2ObjectMap<String> reverseDictionary = new Int2ObjectOpenHashMap<>();
    MutableInt nextId = new MutableInt(0);
    for (List<String> multiValue : values) {
      for (String value : multiValue) {
        int dictId = dictionary.computeIntIfAbsent(value, k -> nextId.getAndIncrement());
        reverseDictionary.putIfAbsent(dictId, value);
      }
    }
    return new NonnullPair<>(dictionary, reverseDictionary);
  }

  private void assertAllowListFiltering(RowSupplier rowSupplier, DimensionSelector selector)
  {
    rowSupplier.set(data.get(0));
    IndexedInts encodedRow = selector.getRow();
    Assert.assertEquals(1, encodedRow.size());
    Assert.assertEquals("val1_1", selector.lookupName(encodedRow.get(0)));

    rowSupplier.set(data.get(1));
    encodedRow = selector.getRow();
    Assert.assertEquals(2, encodedRow.size());
    Assert.assertEquals("val2_2", selector.lookupName(encodedRow.get(0)));
    Assert.assertEquals("val2_3", selector.lookupName(encodedRow.get(1)));

    rowSupplier.set(data.get(2));
    encodedRow = selector.getRow();
    Assert.assertEquals(0, encodedRow.size());
  }

  private void assertDenyListFiltering(RowSupplier rowSupplier, DimensionSelector selector)
  {
    rowSupplier.set(data.get(0));
    IndexedInts encodedRow = selector.getRow();
    Assert.assertEquals(1, encodedRow.size());
    Assert.assertEquals("val1_2", selector.lookupName(encodedRow.get(0)));

    rowSupplier.set(data.get(1));
    encodedRow = selector.getRow();
    Assert.assertEquals(1, encodedRow.size());
    Assert.assertEquals("val2_1", selector.lookupName(encodedRow.get(0)));

    rowSupplier.set(data.get(2));
    encodedRow = selector.getRow();
    Assert.assertEquals(1, encodedRow.size());
    Assert.assertEquals("val3_1", selector.lookupName(encodedRow.get(0)));
  }

  private static class RowSupplier implements Supplier<List<String>>
  {
    private List<String> row;

    public void set(List<String> row)
    {
      this.row = row;
    }

    @Override
    public List<String> get()
    {
      return row;
    }
  }
}
