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
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
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
            new Object[]{1L, 1.0D},
            new Object[]{2L, 2.0D},
            new Object[]{1L, 1.0D},
            new Object[]{2L, 2.0D}
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
              new Object[]{1L, 1.0D},
              new Object[]{2L, 2.0D},
              new Object[]{1L, 1.0D},
              new Object[]{2L, 2.0D}
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
            new Object[]{1L, 1.0D},
            new Object[]{2L, 2.0D}
        ),
        new FrameBasedInlineDataSource(Collections.singletonList(frames.get(0)), SIGNATURE1).getRowsAsSequence()
    );
    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{3.0D, 3L},
            new Object[]{4.0D, 4L}
        ),
        new FrameBasedInlineDataSource(Collections.singletonList(frames.get(1)), SIGNATURE2).getRowsAsSequence()
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
            new Object[]{1L, 1.0D},
            new Object[]{2L, 2.0D}
        ),
        new FrameBasedInlineDataSource(Collections.singletonList(frames.get(0)), SIGNATURE1).getRowsAsSequence()
    );
    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{3.0D, 3L},
            new Object[]{4.0D, 4L}
        ),
        new FrameBasedInlineDataSource(Collections.singletonList(frames.get(1)), SIGNATURE2).getRowsAsSequence()
    );
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
        IntStream.range(1, 1 + numRows).mapToObj(i -> new Object[]{i, (double) i}).collect(Collectors.toList()),
        SIGNATURE1
    );
  }

  // Signature: col1: DOUBLE, col2: LONG
  private static ScanResultValue scanResultValue2(int numRows)
  {
    return new ScanResultValue(
        "dummy",
        ImmutableList.of("col1", "col2"),
        IntStream.range(3, 3 + numRows).mapToObj(i -> new Object[]{(double) i, i}).collect(Collectors.toList()),
        SIGNATURE2
    );
  }
}
