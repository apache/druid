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

package org.apache.druid.msq.input.stage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class StageInputSpecSlicerTest
{
  private static final Int2ObjectMap<ReadablePartitions> STAGE_PARTITIONS_MAP =
      new Int2ObjectOpenHashMap<>(
          ImmutableMap.<Integer, ReadablePartitions>builder()
                      .put(0, ReadablePartitions.striped(0, 2, 2))
                      .put(1, ReadablePartitions.striped(1, 2, 4))
                      .put(2, ReadablePartitions.striped(2, 2, 4))
                      .build()
      );

  private StageInputSpecSlicer slicer;

  @Before
  public void setUp()
  {
    slicer = new StageInputSpecSlicer(STAGE_PARTITIONS_MAP);
  }

  @Test
  public void test_canSliceDynamic()
  {
    Assert.assertFalse(slicer.canSliceDynamic(new StageInputSpec(0)));
  }

  @Test
  public void test_sliceStatic_stageZeroOneSlice()
  {
    Assert.assertEquals(
        Collections.singletonList(
            new StageInputSlice(
                0,
                ReadablePartitions.striped(0, 2, 2)
            )
        ),
        slicer.sliceStatic(new StageInputSpec(0), 1)
    );
  }

  @Test
  public void test_sliceStatic_stageZeroTwoSlices()
  {
    Assert.assertEquals(
        ImmutableList.of(
            new StageInputSlice(
                0,
                new StripedReadablePartitions(0, 2, new IntAVLTreeSet(new int[]{0}))
            ),
            new StageInputSlice(
                0,
                new StripedReadablePartitions(0, 2, new IntAVLTreeSet(new int[]{1}))
            )
        ),
        slicer.sliceStatic(new StageInputSpec(0), 2)
    );
  }

  @Test
  public void test_sliceStatic_stageOneTwoSlices()
  {
    Assert.assertEquals(
        ImmutableList.of(
            new StageInputSlice(
                1,
                new StripedReadablePartitions(1, 2, new IntAVLTreeSet(new int[]{0, 2}))
            ),
            new StageInputSlice(
                1,
                new StripedReadablePartitions(1, 2, new IntAVLTreeSet(new int[]{1, 3}))
            )
        ),
        slicer.sliceStatic(new StageInputSpec(1), 2)
    );
  }

  @Test
  public void test_sliceStatic_notAvailable()
  {
    final IllegalStateException e = Assert.assertThrows(
        IllegalStateException.class,
        () -> slicer.sliceStatic(new StageInputSpec(3), 1)
    );

    MatcherAssert.assertThat(e.getMessage(), CoreMatchers.equalTo("Stage [3] not available"));
  }
}
