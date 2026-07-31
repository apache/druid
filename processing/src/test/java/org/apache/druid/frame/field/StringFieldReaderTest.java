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
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.filter.StringPredicateDruidPredicateFactory;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
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
public class StringFieldReaderTest extends InitializedNullHandlingTest
{
  private static final long MEMORY_POSITION = 1;

  @Mock
  public BaseObjectColumnValueSelector<Object[]> writeSelector;

  private WritableMemory memory;
  private FieldWriter fieldWriter;

  @BeforeEach
  public void setUp()
  {
    memory = WritableMemory.allocate(1000);
    fieldWriter = new StringArrayFieldWriter(writeSelector, false);
  }

  @AfterEach
  public void tearDown()
  {
    fieldWriter.close();
  }

  @Test
  public void test_isNull_nullValue()
  {
    writeToMemory(Collections.singletonList(null));
    Assertions.assertTrue(new StringFieldReader().isNull(memory, MEMORY_POSITION));
    Assertions.assertFalse(new StringArrayFieldReader().isNull(memory, MEMORY_POSITION));
  }

  @Test
  public void test_isNull_twoNullValues()
  {
    writeToMemory(Arrays.asList(null, null));
    Assertions.assertFalse(new StringFieldReader().isNull(memory, MEMORY_POSITION));
    Assertions.assertFalse(new StringArrayFieldReader().isNull(memory, MEMORY_POSITION));
  }

  @Test
  public void test_isNull_nullRow()
  {
    writeToMemory(null);
    Assertions.assertTrue(new StringFieldReader().isNull(memory, MEMORY_POSITION));
    Assertions.assertTrue(new StringArrayFieldReader().isNull(memory, MEMORY_POSITION));
  }

  @Test
  public void test_isNull_emptyString()
  {
    writeToMemory(Collections.singletonList(""));
    Assertions.assertFalse(
        new StringFieldReader().isNull(memory, MEMORY_POSITION)
    );
    Assertions.assertFalse(new StringArrayFieldReader().isNull(memory, MEMORY_POSITION));
  }

  @Test
  public void test_isNull_aValue()
  {
    writeToMemory(Collections.singletonList("foo"));
    Assertions.assertFalse(new StringFieldReader().isNull(memory, MEMORY_POSITION));
    Assertions.assertFalse(new StringArrayFieldReader().isNull(memory, MEMORY_POSITION));
  }

  @Test
  public void test_isNull_multiString()
  {
    writeToMemory(ImmutableList.of("foo", "bar"));
    Assertions.assertFalse(new StringFieldReader().isNull(memory, MEMORY_POSITION));
    Assertions.assertFalse(new StringArrayFieldReader().isNull(memory, MEMORY_POSITION));
  }

  @Test
  public void test_isNull_multiStringIncludingNulls()
  {
    writeToMemory(Arrays.asList(null, "bar"));
    Assertions.assertFalse(new StringFieldReader().isNull(memory, MEMORY_POSITION));
    Assertions.assertFalse(new StringArrayFieldReader().isNull(memory, MEMORY_POSITION));
  }

  @Test
  public void test_makeColumnValueSelector_singleString_notArray()
  {
    writeToMemory(Collections.singletonList("foo"));

    final ColumnValueSelector<?> readSelector =
        new StringFieldReader().makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION, -1));
    final ColumnValueSelector<?> readSelectorAsArray =
        new StringArrayFieldReader().makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION, -1));

    Assertions.assertEquals("foo", readSelector.getObject());
    Assertions.assertArrayEquals(new Object[]{"foo"}, (Object[]) readSelectorAsArray.getObject());
  }

  @Test
  public void test_makeColumnValueSelector_multiString()
  {
    writeToMemory(ImmutableList.of("foo", "bar"));

    final ColumnValueSelector<?> readSelector =
        new StringFieldReader().makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION, -1));
    final ColumnValueSelector<?> readSelectorAsArray =
        new StringArrayFieldReader().makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION, -1));

    Assertions.assertEquals(ImmutableList.of("foo", "bar"), readSelector.getObject());
    Assertions.assertArrayEquals(new Object[]{"foo", "bar"}, (Object[]) readSelectorAsArray.getObject());
  }

  @Test
  public void test_makeColumnValueSelector_null()
  {
    writeToMemory(Collections.singletonList(null));

    final ColumnValueSelector<?> readSelector =
        new StringFieldReader().makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION, -1));
    final ColumnValueSelector<?> readSelectorAsArray =
        new StringArrayFieldReader().makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION, -1));

    Assertions.assertNull(readSelector.getObject());
    Assertions.assertArrayEquals(new Object[]{null}, (Object[]) readSelectorAsArray.getObject());
  }

  @Test
  public void test_makeColumnValueSelector_empty()
  {
    writeToMemory(Collections.emptyList());

    final ColumnValueSelector<?> readSelector =
        new StringFieldReader().makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION, -1));
    final ColumnValueSelector<?> readSelectorAsArray =
        new StringArrayFieldReader().makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION, -1));

    Assertions.assertNull(readSelector.getObject());
    Assertions.assertArrayEquals(ObjectArrays.EMPTY_ARRAY, (Object[]) readSelectorAsArray.getObject());
  }

  @Test
  public void test_makeDimensionSelector_multiString_asArray()
  {
    writeToMemory(ImmutableList.of("foo", "bar"));

    final IllegalStateException e = Assertions.assertThrows(
        IllegalStateException.class,
        () -> new StringArrayFieldReader().makeDimensionSelector(
            memory,
            new ConstantFieldPointer(MEMORY_POSITION, -1),
            null
        )
    );

    MatcherAssert.assertThat(
        e.getMessage(),
        CoreMatchers.containsString("Cannot call makeDimensionSelector")
    );
  }

  @Test
  public void test_makeDimensionSelector_multiString()
  {
    writeToMemory(ImmutableList.of("foo", "bar"));

    final DimensionSelector readSelector =
        new StringFieldReader().makeDimensionSelector(memory, new ConstantFieldPointer(MEMORY_POSITION, -1), null);

    // Data retrieval tests.
    final IndexedInts row = readSelector.getRow();
    Assertions.assertEquals(2, row.size());
    Assertions.assertEquals("foo", readSelector.lookupName(0));
    Assertions.assertEquals("bar", readSelector.lookupName(1));
    Assertions.assertEquals(StringUtils.toUtf8ByteBuffer("foo"), readSelector.lookupNameUtf8(0));
    Assertions.assertEquals(StringUtils.toUtf8ByteBuffer("bar"), readSelector.lookupNameUtf8(1));

    // Informational method tests.
    Assertions.assertTrue(readSelector.supportsLookupNameUtf8());
    Assertions.assertFalse(readSelector.nameLookupPossibleInAdvance());
    Assertions.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, readSelector.getValueCardinality());
    Assertions.assertEquals(Object.class, readSelector.classOfObject());
    Assertions.assertNull(readSelector.idLookup());

    // Value matcher tests.
    Assertions.assertTrue(readSelector.makeValueMatcher("bar").matches(false));
    Assertions.assertFalse(readSelector.makeValueMatcher("baz").matches(false));
    Assertions.assertTrue(readSelector.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("bar")).matches(false));
    Assertions.assertFalse(readSelector.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("baz")).matches(false));
  }

  @Test
  public void test_makeDimensionSelector_multiString_withExtractionFn()
  {
    writeToMemory(ImmutableList.of("foo", "bar"));

    final DimensionSelector readSelector =
        new StringFieldReader().makeDimensionSelector(
            memory,
            new ConstantFieldPointer(MEMORY_POSITION, -1),
            new SubstringDimExtractionFn(1, null)
        );

    // Data retrieval tests.
    final IndexedInts row = readSelector.getRow();
    Assertions.assertEquals(2, row.size());
    Assertions.assertEquals("oo", readSelector.lookupName(0));
    Assertions.assertEquals("ar", readSelector.lookupName(1));

    // Informational method tests.
    Assertions.assertFalse(readSelector.supportsLookupNameUtf8());
    Assertions.assertFalse(readSelector.nameLookupPossibleInAdvance());
    Assertions.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, readSelector.getValueCardinality());
    Assertions.assertEquals(Object.class, readSelector.classOfObject());
    Assertions.assertNull(readSelector.idLookup());

    // Value matcher tests.
    Assertions.assertTrue(readSelector.makeValueMatcher("ar").matches(false));
    Assertions.assertFalse(readSelector.makeValueMatcher("bar").matches(false));
    Assertions.assertTrue(readSelector.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("ar")).matches(false));
    Assertions.assertFalse(readSelector.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("bar")).matches(false));
  }

  private void writeToMemory(@Nullable final List<String> values)
  {
    final Object[] arr = values == null ? null : values.toArray();
    Mockito.when(writeSelector.getObject()).thenReturn(arr);

    if (fieldWriter.writeTo(memory, MEMORY_POSITION, memory.getCapacity() - MEMORY_POSITION) < 0) {
      throw new ISE("Could not write");
    }
  }
}
