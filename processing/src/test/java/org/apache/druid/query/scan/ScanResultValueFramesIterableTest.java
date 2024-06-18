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

package org.apache.druid.query.scan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.FrameBasedInlineDataSource;
import org.apache.druid.query.FrameSignaturePair;
import org.apache.druid.query.QueryToolChestTestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test cases that ensure the correctness of {@link ScanResultValueFramesIterable} in presence of different signatures.
 * There are some more in {@link ScanQueryQueryToolChestTest} that verify the workings of the iterable in bigger picture.
 */
public class ScanResultValueFramesIterableTest extends InitializedNullHandlingTest
{

  private static final RowSignature SIGNATURE1 = RowSignature.builder()
                                                             .add("col1", ColumnType.LONG)
                                                             .add("col2", ColumnType.DOUBLE)
                                                             .build();

  private static final RowSignature SIGNATURE2 = RowSignature.builder()
                                                             .add("col1", ColumnType.DOUBLE)
                                                             .add("col2", ColumnType.LONG)
                                                             .build();

  private static final RowSignature SIGNATURE3 = RowSignature.builder()
                                                             .add("col1", ColumnType.DOUBLE)
                                                             .add("col2", ColumnType.LONG)
                                                             .add("col3", null)
                                                             .build();

  private static final RowSignature SIGNATURE4 = RowSignature.builder()
                                                             .add("col1", ColumnType.DOUBLE)
                                                             .add("col3", null)
                                                             .add("col2", ColumnType.LONG)
                                                             .build();


  @Test
  public void testEmptySequence()
  {
    ScanResultValueFramesIterable iterable = createIterable();
    List<FrameSignaturePair> frames = Lists.newArrayList(iterable);
    Assert.assertEquals(0, frames.size());
  }

  @Test
  public void testAllEmptyScanResultValuesInSequence()
  {

    List<FrameSignaturePair> frames1 = Lists.newArrayList(
        createIterable(
            scanResultValue1(0)
        )
    );
    Assert.assertEquals(0, frames1.size());

    List<FrameSignaturePair> frames2 = Lists.newArrayList(
        createIterable(
            scanResultValue1(0),
            scanResultValue2(0),
            scanResultValue1(0)
        )
    );
    Assert.assertEquals(0, frames2.size());
  }

  @Test
  public void testBatchingWithHomogenousScanResultValues()
  {
    List<FrameSignaturePair> frames = Lists.newArrayList(
        createIterable(
            scanResultValue1(2),
            scanResultValue1(2)
        )
    );
    Assert.assertEquals(1, frames.size());
    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{1000L, 1100.0D},
            new Object[]{1001L, 1101.0D},
            new Object[]{1000L, 1100.0D},
            new Object[]{1001L, 1101.0D}
        ),
        new FrameBasedInlineDataSource(frames, SIGNATURE1).getRowsAsSequence()
    );
  }

  @Test
  public void testBatchingWithHomogenousAndEmptyScanResultValues()
  {
    final List<FrameSignaturePair>[] framesList = new List[3];
    framesList[0] = Lists.newArrayList(
        createIterable(
            scanResultValue1(0),
            scanResultValue1(0),
            scanResultValue1(2),
            scanResultValue1(0),
            scanResultValue1(0),
            scanResultValue1(0),
            scanResultValue1(2),
            scanResultValue1(0)
        )
    );

    framesList[1] = Lists.newArrayList(
        createIterable(
            scanResultValue2(0),
            scanResultValue2(0),
            scanResultValue1(2),
            scanResultValue2(0),
            scanResultValue2(0),
            scanResultValue2(0),
            scanResultValue1(2),
            scanResultValue2(0)
        )
    );

    framesList[2] = Lists.newArrayList(
        createIterable(
            scanResultValue1(0),
            scanResultValue2(0),
            scanResultValue1(2),
            scanResultValue2(0),
            scanResultValue2(0),
            scanResultValue1(0),
            scanResultValue1(2),
            scanResultValue1(0)
        )
    );

    for (List<FrameSignaturePair> frames : framesList) {
      Assert.assertEquals(1, frames.size());
      QueryToolChestTestHelper.assertArrayResultsEquals(
          ImmutableList.of(
              new Object[]{1000L, 1100.0D},
              new Object[]{1001L, 1101.0D},
              new Object[]{1000L, 1100.0D},
              new Object[]{1001L, 1101.0D}
          ),
          new FrameBasedInlineDataSource(frames, SIGNATURE1).getRowsAsSequence()
      );

    }
  }


  @Test
  public void testBatchingWithHeterogenousScanResultValues()
  {
    List<FrameSignaturePair> frames = Lists.newArrayList(
        createIterable(
            scanResultValue1(2),
            scanResultValue2(2)
        )
    );
    Assert.assertEquals(2, frames.size());
    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{1000L, 1100.0D},
            new Object[]{1001L, 1101.0D}
        ),
        new FrameBasedInlineDataSource(frames.subList(0, 1), SIGNATURE1).getRowsAsSequence()
    );
    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{2000.0D, 2100L},
            new Object[]{2001.0D, 2101L}
        ),
        new FrameBasedInlineDataSource(frames.subList(1, 2), SIGNATURE2).getRowsAsSequence()
    );
  }

  @Test
  public void testBatchingWithHeterogenousScanResultValuesAndNullTypes()
  {
    List<FrameSignaturePair> frames = Lists.newArrayList(
        createIterable(
            scanResultValue1(2),
            scanResultValue3(2)
        )
    );
    Assert.assertEquals(2, frames.size());
    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{1000L, 1100.0D},
            new Object[]{1001L, 1101.0D}
        ),
        new FrameBasedInlineDataSource(frames.subList(0, 1), SIGNATURE1).getRowsAsSequence()
    );
    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{3000.0D, 3100L},
            new Object[]{3001.0D, 3101L}
        ),
        new FrameBasedInlineDataSource(frames.subList(1, 2), SIGNATURE2).getRowsAsSequence()
    );
  }

  @Test
  public void testBatchingWithHeterogenousAndEmptyScanResultValues()
  {
    List<FrameSignaturePair> frames = Lists.newArrayList(
        createIterable(
            scanResultValue1(0),
            scanResultValue2(0),
            scanResultValue1(2),
            scanResultValue1(0),
            scanResultValue2(2),
            scanResultValue2(0),
            scanResultValue2(0)
        )
    );
    Assert.assertEquals(2, frames.size());
    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{1000L, 1100.0D},
            new Object[]{1001L, 1101.0D}
        ),
        new FrameBasedInlineDataSource(frames.subList(0, 1), SIGNATURE1).getRowsAsSequence()
    );
    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{2000.0D, 2100L},
            new Object[]{2001.0D, 2101L}
        ),
        new FrameBasedInlineDataSource(frames.subList(1, 2), SIGNATURE2).getRowsAsSequence()
    );
  }

  @Test
  public void testBatchingWithHeterogenousAndEmptyScanResultValuesAndNullTypes()
  {
    List<FrameSignaturePair> frames = Lists.newArrayList(
        createIterable(
            scanResultValue1(0),
            scanResultValue2(0),
            scanResultValue1(2),
            scanResultValue1(0),
            scanResultValue2(2),
            scanResultValue2(0),
            scanResultValue2(0)
        )
    );
    Assert.assertEquals(2, frames.size());
    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{1000L, 1100.0D},
            new Object[]{1001L, 1101.0D}
        ),
        new FrameBasedInlineDataSource(frames.subList(0, 1), SIGNATURE1).getRowsAsSequence()
    );
    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{2000.0D, 2100L},
            new Object[]{2001.0D, 2101L}
        ),
        new FrameBasedInlineDataSource(frames.subList(1, 2), SIGNATURE2).getRowsAsSequence()
    );
  }

  @Test
  public void testBatchingWithDifferentRowSignaturesButSameTrimmedRowSignature()
  {
    List<FrameSignaturePair> frames = Lists.newArrayList(
        createIterable(
            scanResultValue3(0),
            scanResultValue4(0),
            scanResultValue3(2),
            scanResultValue3(0),
            scanResultValue4(2),
            scanResultValue4(0),
            scanResultValue3(0)
        )
    );
    Assert.assertEquals(1, frames.size());
    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{3000.0D, 3100L},
            new Object[]{3001.0D, 3101L},
            new Object[]{4000.0D, 4100L},
            new Object[]{4001.0D, 4101L}
        ),
        new FrameBasedInlineDataSource(frames, SIGNATURE2).getRowsAsSequence()
    );
  }

  @Test
  public void testExceptionThrownWithMissingType()
  {
    Sequence<FrameSignaturePair> frames = Sequences.simple(createIterable(incompleteTypeScanResultValue(1)));
    Assert.assertThrows(DruidException.class, frames::toList);
  }


  @Test
  public void testSplitting()
  {
    List<FrameSignaturePair> frames = Lists.newArrayList(
        createIterable(
            Collections.nCopies(100, scanResultValue1(2)).toArray(new ScanResultValue[0])
        )
    );
    Assert.assertEquals(5, frames.size());
  }

  private static ScanResultValueFramesIterable createIterable(
      ScanResultValue... scanResultValues
  )
  {
    return new ScanResultValueFramesIterable(
        Sequences.simple(Arrays.asList(scanResultValues)),
        new ArenaMemoryAllocatorFactory(1000),
        false,
        null,
        rowSignature -> (Object[] rows) -> rows
    );
  }

  // Signature: col1: LONG, col2: DOUBLE
  private static ScanResultValue scanResultValue1(int numRows)
  {
    return new ScanResultValue(
        "dummy",
        ImmutableList.of("col1", "col2"),
        IntStream.range(1000, 1000 + numRows)
                 .mapToObj(i -> new Object[]{i, (double) i + 100})
                 .collect(Collectors.toList()),
        SIGNATURE1
    );
  }

  // Signature: col1: DOUBLE, col2: LONG
  private static ScanResultValue scanResultValue2(int numRows)
  {
    return new ScanResultValue(
        "dummy",
        ImmutableList.of("col1", "col2"),
        IntStream.range(2000, 2000 + numRows)
                 .mapToObj(i -> new Object[]{(double) i, i + 100})
                 .collect(Collectors.toList()),
        SIGNATURE2
    );
  }

  // Signature: col1: DOUBLE, col2: LONG, col3: null
  private static ScanResultValue scanResultValue3(int numRows)
  {
    return new ScanResultValue(
        "dummy",
        ImmutableList.of("col1", "col2", "col3"),
        IntStream.range(3000, 3000 + numRows)
                 .mapToObj(i -> new Object[]{(double) i, i + 100, null})
                 .collect(Collectors.toList()),
        SIGNATURE3
    );
  }

  // Signature: col1: DOUBLE, col3: null, col2: LONG
  private static ScanResultValue scanResultValue4(int numRows)
  {
    return new ScanResultValue(
        "dummy",
        ImmutableList.of("col1", "col3", "col2"),
        IntStream.range(4000, 4000 + numRows)
                 .mapToObj(i -> new Object[]{(double) i, null, i + 100})
                 .collect(Collectors.toList()),
        SIGNATURE4
    );
  }

  // Contains ScanResultValue with incomplete type, and non-null row
  private static ScanResultValue incompleteTypeScanResultValue(int numRows)
  {
    return new ScanResultValue(
        "dummy",
        ImmutableList.of("col1", "col3", "col2"),
        IntStream.range(5000, 5000 + numRows)
                 .mapToObj(i -> new Object[]{(double) i, i + 100, i + 200})
                 .collect(Collectors.toList()),
        SIGNATURE4
    );
  }
}
