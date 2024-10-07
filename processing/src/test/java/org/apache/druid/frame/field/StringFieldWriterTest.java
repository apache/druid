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
import org.apache.druid.frame.write.InvalidNullByteException;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
  private FieldWriter fieldWriterRemoveNull;
  private FieldWriter fieldWriterUtf8RemoveNull;

  @Before
  public void setUp()
  {
    memory = WritableMemory.allocate(1000);
    fieldWriter = new StringFieldWriter(selector, false);
    fieldWriterUtf8 = new StringFieldWriter(selectorUtf8, false);
    fieldWriterRemoveNull = new StringFieldWriter(selector, true);
    fieldWriterUtf8RemoveNull = new StringFieldWriter(selectorUtf8, true);
  }

  @After
  public void tearDown()
  {
    for (FieldWriter fw : getFieldWriter(FieldWritersType.ALL)) {
      try {
        fw.close();
      }
      catch (Exception ignore) {
      }
    }
  }


  private List<FieldWriter> getFieldWriter(FieldWritersType fieldWritersType)
  {
    if (fieldWritersType == FieldWritersType.NULL_REPLACING) {
      return Arrays.asList(fieldWriterRemoveNull, fieldWriterUtf8RemoveNull);
    } else if (fieldWritersType == FieldWritersType.ALL) {
      return Arrays.asList(fieldWriter, fieldWriterUtf8, fieldWriterRemoveNull, fieldWriterUtf8RemoveNull);
    } else {
      throw new ISE("Handler missing for type:[%s]", fieldWritersType);
    }
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

  @Test
  public void testNullByteReplacement()
  {
    doTest(
        Arrays.asList("abc\u0000", "foo" + NullHandling.emptyToNullIfNeeded("") + "bar", "def"),
        FieldWritersType.NULL_REPLACING
    );
  }

  @Test
  public void testNullByteNotReplaced()
  {
    mockSelectors(Arrays.asList("abc\u0000", "foo" + NullHandling.emptyToNullIfNeeded("") + "bar", "def"));
    Assert.assertThrows(InvalidNullByteException.class, () -> {
      doTestWithSpecificFieldWriter(fieldWriter);
    });
    Assert.assertThrows(InvalidNullByteException.class, () -> {
      doTestWithSpecificFieldWriter(fieldWriterUtf8);
    });
  }

  private void doTest(final List<String> values)
  {
    doTest(values, FieldWritersType.ALL);
  }

  private void doTest(final List<String> values, FieldWritersType fieldWritersType)
  {
    mockSelectors(values);

    List<FieldWriter> fieldWriters = getFieldWriter(fieldWritersType);
    for (FieldWriter fw : fieldWriters) {
      final Object[] valuesRead = doTestWithSpecificFieldWriter(fw);
      List<String> expectedResults = new ArrayList<>(values);
      if (fieldWritersType == FieldWritersType.NULL_REPLACING) {
        expectedResults = expectedResults.stream()
                                         .map(val -> StringUtils.replace(val, "\u0000", ""))
                                         .collect(Collectors.toList());
      }
      Assert.assertEquals("values read", expectedResults, Arrays.asList(valuesRead));
    }
  }

  private Object[] doTestWithSpecificFieldWriter(FieldWriter fieldWriter)
  {
    final long written = writeToMemory(fieldWriter);
    return readFromMemory(written);
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

  private Object[] readFromMemory(final long written)
  {
    final byte[] bytes = new byte[(int) written];
    memory.getByteArray(MEMORY_POSITION, bytes, 0, (int) written);

    final FieldReader fieldReader = FieldReaders.create("columnNameDoesntMatterHere", ColumnType.STRING_ARRAY);
    final ColumnValueSelector<?> selector = fieldReader.makeColumnValueSelector(
        memory,
        new ConstantFieldPointer(
            MEMORY_POSITION,
            -1
        )
    );

    return (Object[]) selector.getObject();
  }

  private enum FieldWritersType
  {
    NULL_REPLACING, // include null replacing writers only
    ALL // include all writers
  }
}
