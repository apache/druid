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
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;

@ParameterizedClass

@MethodSource("constructorFeeder")
public class FrameReaderTest extends InitializedNullHandlingTest
{
  private final FrameType frameType;

  private CursorFactory inputCursorFactory;
  private Frame frame;
  private FrameReader frameReader;

  public FrameReaderTest(final FrameType frameType)
  {
    this.frameType = frameType;
  }

  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    for (final FrameType frameType : FrameType.values()) {
      constructors.add(new Object[]{frameType});
    }

    return constructors;
  }

  @BeforeEach
  public void setUp()
  {
    inputCursorFactory = new QueryableIndexCursorFactory(TestIndex.getNoRollupMMappedTestIndex());

    final FrameSequenceBuilder frameSequenceBuilder =
        FrameSequenceBuilder.fromCursorFactory(inputCursorFactory)
                            .frameType(frameType)
                            .allocator(HeapMemoryAllocator.unlimited());

    frame = Iterables.getOnlyElement(frameSequenceBuilder.frames().toList());
    frameReader = FrameReader.create(frameSequenceBuilder.signature());
  }

  @Test
  public void testSignature()
  {
    Assertions.assertEquals(inputCursorFactory.getRowSignature(), frameReader.signature());
  }

  @Test
  public void testColumnCapabilitiesToColumnType()
  {
    for (final String columnName : inputCursorFactory.getRowSignature().getColumnNames()) {
      Assertions.assertEquals(
          inputCursorFactory.getRowSignature().getColumnCapabilities(columnName).toColumnType(),
          frameReader.columnCapabilities(frame, columnName).toColumnType(),
          columnName
      );
    }
  }
}
