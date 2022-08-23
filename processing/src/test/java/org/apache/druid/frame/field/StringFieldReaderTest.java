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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.RangeIndexedInts;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.List;

public class StringFieldReaderTest extends InitializedNullHandlingTest
{
  private static final long MEMORY_POSITION = 1;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  public DimensionSelector writeSelector;

  private WritableMemory memory;
  private FieldWriter fieldWriter;

  @Before
  public void setUp()
  {
    memory = WritableMemory.allocate(1000);
    fieldWriter = new StringFieldWriter(writeSelector);
  }

  @After
  public void tearDown()
  {
    fieldWriter.close();
  }

  @Test
  public void test_makeColumnValueSelector_singleString_notArray()
  {
    writeToMemory(Collections.singletonList("foo"));

    final ColumnValueSelector<?> readSelector =
        new StringFieldReader(false).makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION));
    final ColumnValueSelector<?> readSelectorAsArray =
        new StringFieldReader(true).makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION));

    Assert.assertEquals("foo", readSelector.getObject());
    Assert.assertEquals(Collections.singletonList("foo"), readSelectorAsArray.getObject());
  }

  @Test
  public void test_makeColumnValueSelector_multiString()
  {
    writeToMemory(ImmutableList.of("foo", "bar"));

    final ColumnValueSelector<?> readSelector =
        new StringFieldReader(false).makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION));
    final ColumnValueSelector<?> readSelectorAsArray =
        new StringFieldReader(true).makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION));

    Assert.assertEquals(ImmutableList.of("foo", "bar"), readSelector.getObject());
    Assert.assertEquals(ImmutableList.of("foo", "bar"), readSelectorAsArray.getObject());
  }

  @Test
  public void test_makeColumnValueSelector_null()
  {
    writeToMemory(Collections.singletonList(null));

    final ColumnValueSelector<?> readSelector =
        new StringFieldReader(false).makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION));
    final ColumnValueSelector<?> readSelectorAsArray =
        new StringFieldReader(true).makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION));

    Assert.assertNull(readSelector.getObject());
    Assert.assertEquals(Collections.singletonList(null), readSelectorAsArray.getObject());
  }

  @Test
  public void test_makeColumnValueSelector_empty()
  {
    writeToMemory(Collections.emptyList());

    final ColumnValueSelector<?> readSelector =
        new StringFieldReader(false).makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION));
    final ColumnValueSelector<?> readSelectorAsArray =
        new StringFieldReader(true).makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION));

    Assert.assertNull(readSelector.getObject());
    Assert.assertEquals(Collections.emptyList(), readSelectorAsArray.getObject());
  }

  @Test
  public void test_makeDimensionSelector_multiString_asArray()
  {
    writeToMemory(ImmutableList.of("foo", "bar"));

    final IllegalStateException e = Assert.assertThrows(
        IllegalStateException.class,
        () -> new StringFieldReader(true).makeDimensionSelector(memory, new ConstantFieldPointer(MEMORY_POSITION), null)
    );

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Cannot call makeDimensionSelector"))
    );
  }

  @Test
  public void test_makeDimensionSelector_multiString()
  {
    writeToMemory(ImmutableList.of("foo", "bar"));

    final DimensionSelector readSelector =
        new StringFieldReader(false).makeDimensionSelector(memory, new ConstantFieldPointer(MEMORY_POSITION), null);

    // Data retrieval tests.
    final IndexedInts row = readSelector.getRow();
    Assert.assertEquals(2, row.size());
    Assert.assertEquals("foo", readSelector.lookupName(0));
    Assert.assertEquals("bar", readSelector.lookupName(1));
    Assert.assertEquals(StringUtils.toUtf8ByteBuffer("foo"), readSelector.lookupNameUtf8(0));
    Assert.assertEquals(StringUtils.toUtf8ByteBuffer("bar"), readSelector.lookupNameUtf8(1));

    // Informational method tests.
    Assert.assertTrue(readSelector.supportsLookupNameUtf8());
    Assert.assertFalse(readSelector.nameLookupPossibleInAdvance());
    Assert.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, readSelector.getValueCardinality());
    Assert.assertEquals(Object.class, readSelector.classOfObject());
    Assert.assertNull(readSelector.idLookup());

    // Value matcher tests.
    Assert.assertTrue(readSelector.makeValueMatcher("bar").matches());
    Assert.assertFalse(readSelector.makeValueMatcher("baz").matches());
    Assert.assertTrue(readSelector.makeValueMatcher("bar"::equals).matches());
    Assert.assertFalse(readSelector.makeValueMatcher("baz"::equals).matches());
  }

  @Test
  public void test_makeDimensionSelector_multiString_withExtractionFn()
  {
    writeToMemory(ImmutableList.of("foo", "bar"));

    final DimensionSelector readSelector =
        new StringFieldReader(false).makeDimensionSelector(
            memory,
            new ConstantFieldPointer(MEMORY_POSITION),
            new SubstringDimExtractionFn(1, null)
        );

    // Data retrieval tests.
    final IndexedInts row = readSelector.getRow();
    Assert.assertEquals(2, row.size());
    Assert.assertEquals("oo", readSelector.lookupName(0));
    Assert.assertEquals("ar", readSelector.lookupName(1));

    // Informational method tests.
    Assert.assertFalse(readSelector.supportsLookupNameUtf8());
    Assert.assertFalse(readSelector.nameLookupPossibleInAdvance());
    Assert.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, readSelector.getValueCardinality());
    Assert.assertEquals(Object.class, readSelector.classOfObject());
    Assert.assertNull(readSelector.idLookup());

    // Value matcher tests.
    Assert.assertTrue(readSelector.makeValueMatcher("ar").matches());
    Assert.assertFalse(readSelector.makeValueMatcher("bar").matches());
    Assert.assertTrue(readSelector.makeValueMatcher("ar"::equals).matches());
    Assert.assertFalse(readSelector.makeValueMatcher("bar"::equals).matches());
  }

  private void writeToMemory(final List<String> values)
  {
    final RangeIndexedInts row = new RangeIndexedInts();
    row.setSize(values.size());

    Mockito.when(writeSelector.getRow()).thenReturn(row);

    if (values.size() > 0) {
      Mockito.when(writeSelector.supportsLookupNameUtf8()).thenReturn(false);
    }

    for (int i = 0; i < values.size(); i++) {
      final String value = values.get(i);
      Mockito.when(writeSelector.lookupName(i)).thenReturn(value);
    }

    if (fieldWriter.writeTo(memory, MEMORY_POSITION, memory.getCapacity() - MEMORY_POSITION) < 0) {
      throw new ISE("Could not write");
    }
  }
}
