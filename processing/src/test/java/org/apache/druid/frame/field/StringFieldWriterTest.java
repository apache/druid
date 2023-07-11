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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.RangeIndexedInts;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class StringFieldWriterTest extends InitializedNullHandlingTest
{
  private static final long MEMORY_POSITION = 1;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  public DimensionSelector selector;

  @Mock
  public DimensionSelector selectorUtf8;

  private WritableMemory memory;
  private FieldWriter fieldWriter;
  private FieldWriter fieldWriterUtf8;

  @Before
  public void setUp()
  {
    memory = WritableMemory.allocate(1000);
    fieldWriter = new StringFieldWriter(selector);
    fieldWriterUtf8 = new StringFieldWriter(selectorUtf8);
  }

  @After
  public void tearDown()
  {
    fieldWriter.close();
    fieldWriterUtf8.close();
  }

  @Test
  public void testEmptyList()
  {
    doTest(Collections.emptyList());
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
    doTest(Arrays.asList("foo", NullHandling.emptyToNullIfNeeded(""), "bar", null));
  }

  private void doTest(final List<String> values)
  {
    mockSelectors(values);

    // Non-UTF8 test
    {
      final long written = writeToMemory(fieldWriter);
      final Object valuesRead = readFromMemory(written);
      Assert.assertEquals("values read (non-UTF8)", values, valuesRead);
    }

    // UTF8 test
    {
      final long writtenUtf8 = writeToMemory(fieldWriterUtf8);
      final Object valuesReadUtf8 = readFromMemory(writtenUtf8);
      Assert.assertEquals("values read (UTF8)", values, valuesReadUtf8);
    }
  }

  private void mockSelectors(final List<String> values)
  {
    final RangeIndexedInts row = new RangeIndexedInts();
    row.setSize(values.size());

    Mockito.when(selector.getRow()).thenReturn(row);
    Mockito.when(selectorUtf8.getRow()).thenReturn(row);

    if (values.size() > 0) {
      Mockito.when(selector.supportsLookupNameUtf8()).thenReturn(false);
      Mockito.when(selectorUtf8.supportsLookupNameUtf8()).thenReturn(true);
    }

    for (int i = 0; i < values.size(); i++) {
      final String value = values.get(i);
      Mockito.when(selector.lookupName(i)).thenReturn(value);

      final ByteBuffer buf;

      if (value == null) {
        buf = null;
      } else {
        // Create a ByteBuffer that extends beyond its position and limit, to verify this case works properly.
        final byte[] valueBytes = StringUtils.toUtf8(value);
        buf = ByteBuffer.allocate(value.length() + 2);
        buf.put(0, (byte) 'X');
        buf.put(buf.capacity() - 1, (byte) 'X');
        buf.position(1);
        buf.put(valueBytes);
        buf.position(1);
        buf.limit(buf.capacity() - 1);
      }

      // Must duplicate: lookupNameUtf8 guarantees that the returned buffer will not be reused.
      Mockito.when(selectorUtf8.lookupNameUtf8(i)).then(invocation -> buf == null ? null : buf.duplicate());
    }
  }

  private long writeToMemory(final FieldWriter writer)
  {
    // Try all maxSizes up until the one that finally works, then return it.
    for (long maxSize = 0; maxSize < memory.getCapacity() - MEMORY_POSITION; maxSize++) {
      final long written = writer.writeTo(memory, MEMORY_POSITION, maxSize);
      if (written > 0) {
        Assert.assertEquals("bytes written", maxSize, written);
        return written;
      }
    }

    throw new ISE("Could not write in memory with capacity [%,d]", memory.getCapacity() - MEMORY_POSITION);
  }

  private List<String> readFromMemory(final long written)
  {
    final byte[] bytes = new byte[(int) written];
    memory.getByteArray(MEMORY_POSITION, bytes, 0, (int) written);

    final FieldReader fieldReader = FieldReaders.create("columnNameDoesntMatterHere", ColumnType.STRING_ARRAY);
    final ColumnValueSelector<?> selector =
        fieldReader.makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION));

    //noinspection unchecked
    return (List<String>) selector.getObject();
  }
}
