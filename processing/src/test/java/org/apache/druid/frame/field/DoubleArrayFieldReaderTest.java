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

import com.google.common.collect.ImmutableList;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.ColumnValueSelector;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class DoubleArrayFieldReaderTest extends InitializedNullHandlingTest
{
  private static final long MEMORY_POSITION = 1;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  public ColumnValueSelector writeSelector;

  private WritableMemory memory;
  private FieldWriter fieldWriter;

  //CHECKSTYLE.OFF: Regexp
  private static final Object[] DOUBLES_ARRAY_1 = new Object[]{
      Double.MAX_VALUE,
      Double.MIN_VALUE,
      null,
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY,
      Double.MIN_NORMAL,
      null,
      0.0d,
      1.234234d,
      Double.NaN,
      -1.344234d,
      129123.123123,
      -21312213.33,
      null,
      1111.0,
      23.0,
      null,
  };

  private static final Object[] DOUBLES_ARRAY_2 = new Object[]{
      null,
      Double.MAX_VALUE,
      12.234234d,
      -21.344234d,
      Double.POSITIVE_INFINITY,
      null,
      Double.MIN_VALUE,
      129123.123123,
      null,
      0.0d,
      Double.MIN_NORMAL,
      1111.0,
      Double.NaN,
      Double.NEGATIVE_INFINITY,
      null,
      -2133.33,
      23.0,
      null
  };
  //CHECKSTYLE.ON: Regexp

  private static final List<Double> DOUBLES_LIST_1;
  private static final List<Double> DOUBLES_LIST_2;

  static {
    DOUBLES_LIST_1 = Arrays.stream(DOUBLES_ARRAY_1).map(val -> (Double) val).collect(Collectors.toList());
    DOUBLES_LIST_2 = Arrays.stream(DOUBLES_ARRAY_2).map(val -> (Double) val).collect(Collectors.toList());
  }

  @Before
  public void setUp()
  {
    memory = WritableMemory.allocate(1000);
    fieldWriter = NumericArrayFieldWriter.getDoubleArrayFieldWriter(writeSelector);
  }

  @After
  public void tearDown()
  {
    fieldWriter.close();
  }

  @Test
  public void test_isNull_null()
  {
    writeToMemory(null, MEMORY_POSITION);
    Assert.assertTrue(new DoubleArrayFieldReader().isNull(memory, MEMORY_POSITION));
  }

  @Test
  public void test_isNull_aValue()
  {
    writeToMemory(DOUBLES_ARRAY_1, MEMORY_POSITION);
    Assert.assertFalse(new DoubleArrayFieldReader().isNull(memory, MEMORY_POSITION));
  }

  @Test
  public void test_isNull_emptyArray()
  {
    writeToMemory(new Object[]{}, MEMORY_POSITION);
    Assert.assertFalse(new DoubleArrayFieldReader().isNull(memory, MEMORY_POSITION));
  }

  @Test
  public void test_isNull_arrayWithSingleNullElement()
  {
    writeToMemory(new Object[]{null}, MEMORY_POSITION);
    Assert.assertFalse(new DoubleArrayFieldReader().isNull(memory, MEMORY_POSITION));
  }

  @Test
  public void test_makeColumnValueSelector_null()
  {
    long sz = writeToMemory(null, MEMORY_POSITION);

    final ColumnValueSelector<?> readSelector =
        new DoubleArrayFieldReader().makeColumnValueSelector(
            memory,
            new ConstantFieldPointer(MEMORY_POSITION, sz)
        );

    Assert.assertTrue(readSelector.isNull());
  }

  @Test
  public void test_makeColumnValueSelector_aValue()
  {
    long sz = writeToMemory(DOUBLES_ARRAY_1, MEMORY_POSITION);

    final ColumnValueSelector<?> readSelector =
        new DoubleArrayFieldReader()
            .makeColumnValueSelector(
                memory,
                new ConstantFieldPointer(MEMORY_POSITION, sz)
            );

    assertResults(DOUBLES_LIST_1, readSelector.getObject());
  }

  @Test
  public void test_makeColumnValueSelector_multipleValues()
  {
    long sz = writeToMemory(DOUBLES_ARRAY_1, MEMORY_POSITION);
    long sz2 = writeToMemory(DOUBLES_ARRAY_2, MEMORY_POSITION + sz);
    IndexArrayFieldPointer pointer = new IndexArrayFieldPointer(
        ImmutableList.of(MEMORY_POSITION, MEMORY_POSITION + sz),
        ImmutableList.of(sz, sz2)
    );

    final ColumnValueSelector<?> readSelector = new DoubleArrayFieldReader().makeColumnValueSelector(memory, pointer);
    pointer.setPointer(0);
    assertResults(DOUBLES_LIST_1, readSelector.getObject());
    pointer.setPointer(1);
    assertResults(DOUBLES_LIST_2, readSelector.getObject());
  }

  @Test
  public void test_makeColumnValueSelector_emptyArray()
  {
    long sz = writeToMemory(new Object[]{}, MEMORY_POSITION);

    final ColumnValueSelector<?> readSelector =
        new DoubleArrayFieldReader()
            .makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION, sz));

    assertResults(Collections.emptyList(), readSelector.getObject());
  }

  @Test
  public void test_makeColumnValueSelector_arrayWithSingleNullElement()
  {
    long sz = writeToMemory(new Object[]{null}, MEMORY_POSITION);

    final ColumnValueSelector<?> readSelector =
        new DoubleArrayFieldReader().makeColumnValueSelector(
            memory,
            new ConstantFieldPointer(MEMORY_POSITION, sz)
        );

    assertResults(Collections.singletonList(null), readSelector.getObject());
  }

  private long writeToMemory(final Object value, final long initialPosition)
  {
    Mockito.when(writeSelector.getObject()).thenReturn(value);

    long bytesWritten = fieldWriter.writeTo(memory, initialPosition, memory.getCapacity() - initialPosition);
    if (bytesWritten < 0) {
      throw new ISE("Could not write");
    }
    return bytesWritten;
  }

  private void assertResults(List<Double> expected, Object actual)
  {
    if (expected == null) {
      Assert.assertNull(actual);
    }
    Assert.assertTrue(actual instanceof Object[]);
    List<Double> actualList = new ArrayList<>();
    for (Object val : (Object[]) actual) {
      actualList.add((Double) val);
    }

    Assert.assertEquals(expected, actualList);
  }
}
