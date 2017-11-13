/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Sets;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.data.input.MapBasedInputRow;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Comparators;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.data.IncrementalIndexTest;
import io.druid.segment.incremental.IncrementalIndex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.roaringbitmap.IntIterator;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IndexMergerNullHandlingTest
{
  private IndexMerger indexMerger;
  private IndexIO indexIO;
  private IndexSpec indexSpec;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp()
  {
    indexMerger = TestHelper.getTestIndexMergerV9();
    indexIO = TestHelper.getTestIndexIO();
    indexSpec = new IndexSpec();
  }

  @Test
  public void testStringColumnNullHandling() throws Exception
  {
    List<Map<String, Object>> nonNullFlavors = new ArrayList<>();
    nonNullFlavors.add(ImmutableMap.of("d", "a"));
    nonNullFlavors.add(ImmutableMap.of("d", ImmutableList.of("a", "b")));

    List<Map<String, Object>> nullFlavors = new ArrayList<>();
    Map<String, Object> mMissing = ImmutableMap.of();
    Map<String, Object> mEmptyList = ImmutableMap.of("d", Collections.emptyList());
    Map<String, Object> mNull = new HashMap<>();
    mNull.put("d", null);
    Map<String, Object> mEmptyString = ImmutableMap.of("d", "");
    Map<String, Object> mListOfNull = ImmutableMap.of("d", Collections.singletonList(null));
    Map<String, Object> mListOfEmptyString = ImmutableMap.of("d", Collections.singletonList(""));

    nullFlavors.add(mMissing);
    nullFlavors.add(mEmptyList);
    nullFlavors.add(mNull);
    nullFlavors.add(mEmptyString);
    nullFlavors.add(mListOfNull);
    nullFlavors.add(mListOfEmptyString);

    Set<Map<String, Object>> allValues = new HashSet<>();
    allValues.addAll(nonNullFlavors);
    allValues.addAll(nullFlavors);

    for (Set<Map<String, Object>> subset : Sets.powerSet(allValues)) {
      if (subset.isEmpty()) {
        continue;
      }

      final List<Map<String, Object>> subsetList = new ArrayList<>(subset);

      IncrementalIndex toPersist = IncrementalIndexTest.createIndex(new AggregatorFactory[]{});
      for (Map<String, Object> m : subsetList) {
        toPersist.add(new MapBasedInputRow(0L, ImmutableList.of("d"), m));
      }

      final File tempDir = temporaryFolder.newFolder();
      try (QueryableIndex index = indexIO.loadIndex(indexMerger.persist(toPersist, tempDir, indexSpec))) {
        final Column column = index.getColumn("d");

        if (subsetList.stream().allMatch(nullFlavors::contains)) {
          // all null -> should be missing
          Assert.assertNull(subsetList.toString(), column);
        } else {
          Assert.assertNotNull(subsetList.toString(), column);

          // The column has multiple values if there are any lists with > 1 element in the input set.
          final boolean hasMultipleValues = subsetList.stream()
                                                      .anyMatch(m -> m.get("d") instanceof List
                                                                     && (((List) m.get("d")).size() > 1));

          // Compute all unique values, the same way that IndexMerger is expected to do it.
          final Set<String> uniqueValues = new HashSet<>();
          for (Map<String, Object> m : subsetList) {
            final List<String> dValues = normalize(m.get("d"), hasMultipleValues);
            uniqueValues.addAll(dValues);

            if (nullFlavors.contains(m)) {
              uniqueValues.add(null);
            }
          }

          try (final DictionaryEncodedColumn<String> dictionaryColumn = column.getDictionaryEncoding()) {
            // Verify unique values against the dictionary.
            Assert.assertEquals(
                subsetList.toString(),
                uniqueValues.stream().sorted(Comparators.naturalNullsFirst()).collect(Collectors.toList()),
                IntStream.range(0, dictionaryColumn.getCardinality())
                         .mapToObj(dictionaryColumn::lookupName)
                         .collect(Collectors.toList())
            );

            Assert.assertEquals(subsetList.toString(), hasMultipleValues, dictionaryColumn.hasMultipleValues());
            Assert.assertEquals(subsetList.toString(), uniqueValues.size(), dictionaryColumn.getCardinality());

            // Verify the expected set of rows was indexed, ignoring order.
            Assert.assertEquals(
                subsetList.toString(),
                ImmutableMultiset.copyOf(
                    subsetList.stream()
                              .map(m -> normalize(m.get("d"), hasMultipleValues))
                              .distinct() // Distinct values only, because we expect rollup.
                              .collect(Collectors.toList())
                ),
                ImmutableMultiset.copyOf(
                    IntStream.range(0, index.getNumRows())
                             .mapToObj(rowNumber -> getRow(dictionaryColumn, rowNumber))
                             // The "distinct" shouldn't be necessary, but it is, because [{}, {d=}, {d=a}]
                             // yields [[null] x 2, [a]] (arguably a bug).
                             .distinct()
                             .collect(Collectors.toList())
                )
            );

            // Verify that the bitmap index for null is correct.
            final BitmapIndex bitmapIndex = column.getBitmapIndex();

            // Read through the column to find all the rows that should match null.
            final List<Integer> expectedNullRows = new ArrayList<>();
            for (int i = 0; i < index.getNumRows(); i++) {
              final List<String> row = getRow(dictionaryColumn, i);
              if (row.isEmpty() || row.stream().anyMatch(Strings::isNullOrEmpty)) {
                expectedNullRows.add(i);
              }
            }

            Assert.assertEquals(subsetList.toString(), expectedNullRows.size() > 0, bitmapIndex.hasNulls());

            if (expectedNullRows.size() > 0) {
              Assert.assertEquals(subsetList.toString(), 0, bitmapIndex.getIndex(null));

              final ImmutableBitmap nullBitmap = bitmapIndex.getBitmap(bitmapIndex.getIndex(null));
              final List<Integer> actualNullRows = new ArrayList<>();
              final IntIterator iterator = nullBitmap.iterator();
              while (iterator.hasNext()) {
                actualNullRows.add(iterator.next());
              }

              Assert.assertEquals(subsetList.toString(), expectedNullRows, actualNullRows);
            } else {
              Assert.assertEquals(-1, bitmapIndex.getIndex(null));
            }
          }
        }
      }
    }
  }

  /**
   * Normalize an input value the same way that IndexMerger is expected to do it.
   */
  private static List<String> normalize(final Object value, final boolean hasMultipleValues)
  {
    final List<String> retVal = new ArrayList<>();

    if (value == null) {
      if (!hasMultipleValues) {
        // nulls become nulls in single valued columns, but are empty lists in multi valued columns
        retVal.add(null);
      }
    } else if (value instanceof String) {
      retVal.add(Strings.emptyToNull(((String) value)));
    } else if (value instanceof List) {
      final List<String> list = (List<String>) value;
      if (list.isEmpty() && !hasMultipleValues) {
        // empty lists become nulls in single valued columns
        retVal.add(null);
      } else {
        retVal.addAll(list.stream().map(Strings::emptyToNull).collect(Collectors.toList()));
      }
    } else {
      throw new ISE("didn't expect class[%s]", value.getClass());
    }

    return retVal;
  }

  /**
   * Get a particular row from a column, exactly as reported by the column.
   */
  private static List<String> getRow(final DictionaryEncodedColumn<String> column, final int rowNumber)
  {
    final List<String> retVal = new ArrayList<>();

    if (column.hasMultipleValues()) {
      column.getMultiValueRow(rowNumber).forEach(i -> retVal.add(column.lookupName(i)));
    } else {
      retVal.add(column.lookupName(column.getSingleValueRow(rowNumber)));
    }

    return retVal;
  }
}
