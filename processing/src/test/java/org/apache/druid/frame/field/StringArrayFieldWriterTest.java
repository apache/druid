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

package org.apache.druid.frame.field;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.frame.FrameType;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class StringArrayFieldWriterTest extends InitializedNullHandlingTest
{
  private static final long MEMORY_POSITION = 1;

  @Mock
  public BaseObjectColumnValueSelector<Object[]> selector;

  private WritableMemory memory;
  private FieldWriter fieldWriter;

  @BeforeEach
  public void setUp()
  {
    memory = WritableMemory.allocate(1000);
    fieldWriter = new StringArrayFieldWriter(selector, false);
  }

  @AfterEach
  public void tearDown()
  {
    fieldWriter.close();
  }

  @Test
  public void testEmptyArray()
  {
    doTest(Collections.emptyList());
  }

  @Test
  public void testNullArray()
  {
    doTest(null);
  }

  @Test
  public void testOneString()
  {
    doTest(Collections.singletonList("foo"));
  }

  @Test
  public void testOneNull()
  {
    doTest(Collections.singletonList(null));
  }

  @Test
  public void testMultiValueString()
  {
    doTest(Arrays.asList("foo", "bar"));
  }

  @Test
  public void testMultiValueStringContainingNulls()
  {
    doTest(Arrays.asList("foo", "", "bar", null));
  }

  private void doTest(@Nullable final List<String> values)
  {
    mockSelector(values);

    final long written = writeToMemory(fieldWriter);
    final Object valuesRead = readFromMemory(written);
    Assertions.assertEquals(values, valuesRead, "values read");
  }

  private void mockSelector(@Nullable final List<String> values)
  {
    final Object[] arr = values == null ? null : values.toArray();
    Mockito.when(selector.getObject()).thenReturn(arr);
  }

  private long writeToMemory(final FieldWriter writer)
  {
    // Try all maxSizes up until the one that finally works, then return it.
    for (long maxSize = 0; maxSize < memory.getCapacity() - MEMORY_POSITION; maxSize++) {
      final long written = writer.writeTo(memory, MEMORY_POSITION, maxSize);
      if (written > 0) {
        Assertions.assertEquals(maxSize, written, "bytes written");
        return written;
      }
    }

    throw new ISE("Could not write in memory with capacity [%,d]", memory.getCapacity() - MEMORY_POSITION);
  }

  @Nullable
  private List<String> readFromMemory(final long written)
  {
    final byte[] bytes = new byte[(int) written];
    memory.getByteArray(MEMORY_POSITION, bytes, 0, (int) written);

    final FieldReader fieldReader =
        FieldReaders.create("columnNameDoesntMatterHere", ColumnType.STRING_ARRAY, FrameType.latestRowBased());
    final ColumnValueSelector<?> selector =
        fieldReader.makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION, -1));

    final Object o = selector.getObject();
    //noinspection rawtypes,unchecked
    return o == null ? null : (List) Arrays.asList((Object[]) o);
  }
}
