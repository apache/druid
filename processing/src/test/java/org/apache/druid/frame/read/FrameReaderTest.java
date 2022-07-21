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

package org.apache.druid.frame.read;

import com.google.common.collect.Iterables;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

@RunWith(Parameterized.class)
public class FrameReaderTest extends InitializedNullHandlingTest
{
  private final FrameType frameType;

  private StorageAdapter inputAdapter;
  private Frame frame;
  private FrameReader frameReader;

  public FrameReaderTest(final FrameType frameType)
  {
    this.frameType = frameType;
  }

  @Parameterized.Parameters(name = "frameType = {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    for (final FrameType frameType : FrameType.values()) {
      constructors.add(new Object[]{frameType});
    }

    return constructors;
  }

  @Before
  public void setUp()
  {
    inputAdapter = new QueryableIndexStorageAdapter(TestIndex.getNoRollupMMappedTestIndex());

    final FrameSequenceBuilder frameSequenceBuilder =
        FrameSequenceBuilder.fromAdapter(inputAdapter)
                            .frameType(frameType)
                            .allocator(HeapMemoryAllocator.unlimited());

    frame = Iterables.getOnlyElement(frameSequenceBuilder.frames().toList());
    frameReader = FrameReader.create(frameSequenceBuilder.signature());
  }

  @Test
  public void testSignature()
  {
    Assert.assertEquals(inputAdapter.getRowSignature(), frameReader.signature());
  }

  @Test
  public void testColumnCapabilitiesToColumnType()
  {
    for (final String columnName : inputAdapter.getRowSignature().getColumnNames()) {
      Assert.assertEquals(
          columnName,
          inputAdapter.getRowSignature().getColumnCapabilities(columnName).toColumnType(),
          frameReader.columnCapabilities(frame, columnName).toColumnType()
      );
    }
  }
}
