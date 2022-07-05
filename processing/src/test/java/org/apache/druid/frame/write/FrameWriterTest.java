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

package org.apache.druid.frame.write;

import com.google.common.collect.Iterables;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.allocation.MemoryAllocator;
import org.apache.druid.frame.key.KeyTestUtils;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.key.RowKeyComparator;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameSegment;
import org.apache.druid.frame.segment.FrameStorageAdapter;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests that exercise {@link FrameWriter} implementations.
 */
@RunWith(Parameterized.class)
public class FrameWriterTest extends InitializedNullHandlingTest
{
  private static final int DEFAULT_ALLOCATOR_CAPACITY = 1_000_000;

  @Nullable
  private final FrameType inputFrameType;
  private final FrameType outputFrameType;
  private final FrameWriterTestData.Sortedness sortedness;

  private MemoryAllocator allocator;

  public FrameWriterTest(
      @Nullable final FrameType inputFrameType,
      final FrameType outputFrameType,
      final FrameWriterTestData.Sortedness sortedness
  )
  {
    this.inputFrameType = inputFrameType;
    this.outputFrameType = outputFrameType;
    this.sortedness = sortedness;
    this.allocator = ArenaMemoryAllocator.createOnHeap(DEFAULT_ALLOCATOR_CAPACITY);
  }

  @Parameterized.Parameters(name = "inputFrameType = {0}, outputFrameType = {1}, sorted = {2}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    final Iterable<FrameType> inputFrameTypes = Iterables.concat(
        Collections.singletonList(null), // null means input is not a frame
        Arrays.asList(FrameType.values())
    );

    for (final FrameType inputFrameType : inputFrameTypes) {
      for (final FrameType outputFrameType : FrameType.values()) {
        for (final FrameWriterTestData.Sortedness sortedness : FrameWriterTestData.Sortedness.values()) {
          // Only do sortedness tests for row-based frames. (Columnar frames cannot be sorted.)
          if (sortedness == FrameWriterTestData.Sortedness.UNSORTED || outputFrameType == FrameType.ROW_BASED) {
            constructors.add(new Object[]{inputFrameType, outputFrameType, sortedness});
          }
        }
      }
    }

    return constructors;
  }

  @BeforeClass
  public static void setUpClass()
  {
    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde());
  }

  @Test
  public void test_string()
  {
    testWithDataset(FrameWriterTestData.TEST_STRINGS_SINGLE_VALUE);
  }

  @Test
  public void test_multiValueString()
  {
    testWithDataset(FrameWriterTestData.TEST_STRINGS_MULTI_VALUE);
  }

  @Test
  public void test_arrayString()
  {
    testWithDataset(FrameWriterTestData.TEST_ARRAYS_STRING);
  }

  @Test
  public void test_long()
  {
    testWithDataset(FrameWriterTestData.TEST_LONGS);
  }

  @Test
  public void test_float()
  {
    testWithDataset(FrameWriterTestData.TEST_FLOATS);
  }

  @Test
  public void test_double()
  {
    testWithDataset(FrameWriterTestData.TEST_DOUBLES);
  }

  @Test
  public void test_complex()
  {
    // Complex types can't be sorted, so skip the sortedness tests.
    Assume.assumeThat(sortedness, CoreMatchers.is(FrameWriterTestData.Sortedness.UNSORTED));
    testWithDataset(FrameWriterTestData.TEST_COMPLEX);
  }

  @Test
  public void test_typePairs()
  {
    // Test all possible arrangements of two different types.
    for (final FrameWriterTestData.Dataset<?> dataset1 : FrameWriterTestData.DATASETS) {
      for (final FrameWriterTestData.Dataset<?> dataset2 : FrameWriterTestData.DATASETS) {
        final RowSignature signature = makeSignature(Arrays.asList(dataset1, dataset2));
        final Sequence<List<Object>> rowSequence = unsortAndMakeRows(Arrays.asList(dataset1, dataset2));

        // Sort by all columns up to the first COMPLEX one. (Can't sort by COMPLEX.)
        final List<String> sortColumns = new ArrayList<>();
        if (!dataset1.getType().is(ValueType.COMPLEX)) {
          sortColumns.add(signature.getColumnName(0));

          if (!dataset2.getType().is(ValueType.COMPLEX)) {
            sortColumns.add(signature.getColumnName(1));
          }
        }

        try {
          final Pair<Frame, Integer> writeResult = writeFrame(rowSequence, signature, sortColumns);
          Assert.assertEquals(rowSequence.toList().size(), (int) writeResult.rhs);
          verifyFrame(sortIfNeeded(rowSequence, signature, sortColumns), writeResult.lhs, signature);
        }
        catch (AssertionError e) {
          throw new AssertionError(
              StringUtils.format(
                  "Assert failed in test (%s, %s)",
                  dataset1.getType(),
                  dataset2.getType()
              ),
              e
          );
        }
        catch (Throwable e) {
          throw new RE(e, "Exception in test (%s, %s)", dataset1.getType(), dataset2.getType());
        }
      }
    }
  }

  @Test
  public void test_insufficientWriteCapacity()
  {
    // Test every possible capacity, up to the amount required to write all items from every list.
    final RowSignature signature = makeSignature(FrameWriterTestData.DATASETS);
    final Sequence<List<Object>> rowSequence = unsortAndMakeRows(FrameWriterTestData.DATASETS);
    final int totalRows = rowSequence.toList().size();

    // Sort by all columns up to the first COMPLEX one. (Can't sort by COMPLEX.)
    final List<String> sortColumns = new ArrayList<>();
    for (int i = 0; i < signature.size(); i++) {
      if (signature.getColumnType(i).get().is(ValueType.COMPLEX)) {
        break;
      } else {
        sortColumns.add(signature.getColumnName(i));
      }
    }

    final ByteBuffer allocatorMemory = ByteBuffer.wrap(new byte[DEFAULT_ALLOCATOR_CAPACITY]);

    boolean didWritePartial = false;
    int allocatorSize = 0;

    Pair<Frame, Integer> writeResult;

    do {
      allocatorMemory.limit(allocatorSize);
      allocatorMemory.position(0);
      allocator = ArenaMemoryAllocator.create(allocatorMemory);

      try {
        writeResult = writeFrame(rowSequence, signature, sortColumns);

        final int rowsWritten = writeResult.rhs;

        if (writeResult.rhs > 0 && writeResult.rhs < totalRows) {
          didWritePartial = true;

          verifyFrame(
              sortIfNeeded(rowSequence.limit(rowsWritten), signature, sortColumns),
              writeResult.lhs,
              signature
          );
        }
      }
      catch (Throwable e) {
        throw new RE(e, "Exception while writing with allocatorSize = %s", allocatorSize);
      }

      allocatorSize++;
    } while (writeResult.rhs != totalRows);

    verifyFrame(sortIfNeeded(rowSequence, signature, sortColumns), writeResult.lhs, signature);

    // We expect that at some point in this test, a partial frame would have been written. If not: that's strange
    // and may mean the test isn't testing the right thing.
    Assert.assertTrue("did write a partial frame", didWritePartial);
  }

  /**
   * Verifies that a frame has a certain set of expected rows. The set of expected rows will be reordered according
   * to the current {@link #sortedness} parameter.
   */
  private void verifyFrame(
      final Sequence<List<Object>> expectedRows,
      final Frame frame,
      final RowSignature signature
  )
  {
    final FrameStorageAdapter frameAdapter = new FrameStorageAdapter(
        frame,
        FrameReader.create(signature),
        Intervals.ETERNITY
    );

    FrameTestUtil.assertRowsEqual(
        expectedRows,
        FrameTestUtil.readRowsFromAdapter(frameAdapter, signature, false)
    );
  }

  /**
   * Sort according to the current {@link #sortedness} parameter.
   */
  private Sequence<List<Object>> sortIfNeeded(
      final Sequence<List<Object>> rows,
      final RowSignature signature,
      final List<String> sortColumnNames
  )
  {
    final List<SortColumn> sortColumns = computeSortColumns(sortColumnNames);

    if (sortColumns.isEmpty()) {
      return rows;
    }

    final RowSignature keySignature = KeyTestUtils.createKeySignature(sortColumns, signature);
    final Comparator<RowKey> keyComparator = RowKeyComparator.create(sortColumns);

    return Sequences.sort(
        rows,
        Comparator.comparing(
            row -> KeyTestUtils.createKey(keySignature, row.toArray()),
            keyComparator
        )
    );
  }

  /**
   * Writes as many rows to a single frame as possible. Returns the number of rows written.
   */
  private Pair<Frame, Integer> writeFrame(
      final Sequence<List<Object>> rows,
      final RowSignature signature,
      final List<String> sortColumns
  )
  {
    return writeFrame(
        inputFrameType,
        outputFrameType,
        allocator,
        rows,
        signature,
        computeSortColumns(sortColumns)
    );
  }

  /**
   * Converts the provided column names into {@link SortColumn} according to the current {@link #sortedness}
   * parameter.
   */
  private List<SortColumn> computeSortColumns(final List<String> sortColumnNames)
  {
    if (sortedness == FrameWriterTestData.Sortedness.UNSORTED) {
      return Collections.emptyList();
    } else {
      return sortColumnNames.stream()
                            .map(
                                columnName ->
                                    new SortColumn(columnName, sortedness == FrameWriterTestData.Sortedness.DESCENDING)
                            )
                            .collect(Collectors.toList());
    }
  }

  private <T> void testWithDataset(final FrameWriterTestData.Dataset<T> dataset)
  {
    final List<T> data = dataset.getData(FrameWriterTestData.Sortedness.UNSORTED);
    final RowSignature signature = RowSignature.builder().add("x", dataset.getType()).build();
    final Sequence<List<Object>> rowSequence = rows(data);
    final Pair<Frame, Integer> writeResult = writeFrame(rowSequence, signature, signature.getColumnNames());

    Assert.assertEquals(data.size(), (int) writeResult.rhs);
    verifyFrame(rows(dataset.getData(sortedness)), writeResult.lhs, signature);
  }

  /**
   * Writes as many rows to a single frame as possible. Returns the number of rows written.
   */
  private static Pair<Frame, Integer> writeFrame(
      @Nullable final FrameType inputFrameType,
      final FrameType outputFrameType,
      final MemoryAllocator allocator,
      final Sequence<List<Object>> rows,
      final RowSignature signature,
      final List<SortColumn> sortColumns
  )
  {
    final Segment inputSegment;

    if (inputFrameType == null) {
      // inputFrameType null means input is not a frame
      inputSegment = new RowBasedSegment<>(
          SegmentId.dummy("dummy"),
          rows,
          columnName -> {
            final int columnNumber = signature.indexOf(columnName);
            return row -> columnNumber >= 0 ? row.get(columnNumber) : null;
          },
          signature
      );
    } else {
      final Frame inputFrame = writeFrame(
          null,
          inputFrameType,
          HeapMemoryAllocator.unlimited(),
          rows,
          signature,
          Collections.emptyList()
      ).lhs;

      inputSegment = new FrameSegment(inputFrame, FrameReader.create(signature), SegmentId.dummy("xxx"));
    }

    return inputSegment.asStorageAdapter()
                       .makeCursors(null, Intervals.ETERNITY, VirtualColumns.EMPTY, Granularities.ALL, false, null)
                       .accumulate(
                           null,
                           (retVal, cursor) -> {
                             int numRows = 0;
                             final FrameWriter frameWriter =
                                 FrameWriters.makeFrameWriterFactory(outputFrameType, allocator, signature, sortColumns)
                                             .newFrameWriter(cursor.getColumnSelectorFactory());

                             while (!cursor.isDone() && frameWriter.addSelection()) {
                               numRows++;
                               cursor.advance();
                             }

                             return Pair.of(Frame.wrap(frameWriter.toByteArray()), numRows);
                           }
                       );
  }

  /**
   * Returns a filler value for "type" if "o" is null. Used to pad value lists to the correct length.
   */
  @Nullable
  private static Object fillerValueForType(final ValueType type)
  {
    switch (type) {
      case LONG:
        return NullHandling.defaultLongValue();
      case FLOAT:
        return NullHandling.defaultFloatValue();
      case DOUBLE:
        return NullHandling.defaultDoubleValue();
      case ARRAY:
        return Collections.emptyList();
      default:
        return null;
    }
  }

  /**
   * Create a row signature out of columnar lists of values.
   */
  private static RowSignature makeSignature(final List<FrameWriterTestData.Dataset<?>> datasets)
  {
    final RowSignature.Builder signatureBuilder = RowSignature.builder();

    for (int i = 0; i < datasets.size(); i++) {
      final FrameWriterTestData.Dataset<?> dataset = datasets.get(i);
      signatureBuilder.add(StringUtils.format("col%03d", i), dataset.getType());
    }

    return signatureBuilder.build();
  }

  /**
   * Create rows out of shuffled (unsorted) datasets.
   */
  private static Sequence<List<Object>> unsortAndMakeRows(final List<FrameWriterTestData.Dataset<?>> datasets)
  {
    final List<List<Object>> retVal = new ArrayList<>();

    final int rowSize = datasets.size();
    final List<Iterator<?>> iterators =
        datasets.stream()
                .map(dataset -> dataset.getData(FrameWriterTestData.Sortedness.UNSORTED).iterator())
                .collect(Collectors.toList());

    while (iterators.stream().anyMatch(Iterator::hasNext)) {
      final List<Object> row = new ArrayList<>(rowSize);

      for (int i = 0; i < rowSize; i++) {
        if (iterators.get(i).hasNext()) {
          row.add(iterators.get(i).next());
        } else {
          row.add(fillerValueForType(datasets.get(i).getType().getType()));
        }
      }

      retVal.add(row);
    }

    return Sequences.simple(retVal);
  }

  /**
   * Create a sequence of rows from a list of values. Each value appears in its own row.
   */
  private static Sequence<List<Object>> rows(final List<?> vals)
  {
    final List<List<Object>> retVal = new ArrayList<>();

    for (final Object val : vals) {
      retVal.add(Collections.singletonList(val));
    }

    return Sequences.simple(retVal);
  }
}
