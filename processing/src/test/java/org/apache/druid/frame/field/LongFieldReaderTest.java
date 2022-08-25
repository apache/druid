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
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;
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

import java.util.Objects;

public class LongFieldReaderTest extends InitializedNullHandlingTest
{
  private static final long MEMORY_POSITION = 1;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  public BaseLongColumnValueSelector writeSelector;

  private WritableMemory memory;
  private FieldWriter fieldWriter;

  @Before
  public void setUp()
  {
    memory = WritableMemory.allocate(1000);
    fieldWriter = new LongFieldWriter(writeSelector);
  }

  @After
  public void tearDown()
  {
    fieldWriter.close();
  }

  @Test
  public void test_makeColumnValueSelector_defaultOrNull()
  {
    writeToMemory(NullHandling.defaultLongValue());

    final ColumnValueSelector<?> readSelector =
        new LongFieldReader().makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION));

    Assert.assertEquals(!NullHandling.replaceWithDefault(), readSelector.isNull());

    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals((long) NullHandling.defaultLongValue(), readSelector.getLong());
    }
  }

  @Test
  public void test_makeColumnValueSelector_aValue()
  {
    writeToMemory(5L);

    final ColumnValueSelector<?> readSelector =
        new LongFieldReader().makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION));

    Assert.assertEquals(5L, readSelector.getObject());
  }

  @Test
  public void test_makeDimensionSelector_defaultOrNull()
  {
    writeToMemory(NullHandling.defaultLongValue());

    final DimensionSelector readSelector =
        new LongFieldReader().makeDimensionSelector(memory, new ConstantFieldPointer(MEMORY_POSITION), null);

    // Data retrieval tests.
    final IndexedInts row = readSelector.getRow();
    Assert.assertEquals(1, row.size());
    Assert.assertEquals(NullHandling.replaceWithDefault() ? "0" : null, readSelector.lookupName(0));

    // Informational method tests.
    Assert.assertFalse(readSelector.supportsLookupNameUtf8());
    Assert.assertFalse(readSelector.nameLookupPossibleInAdvance());
    Assert.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, readSelector.getValueCardinality());
    Assert.assertEquals(String.class, readSelector.classOfObject());
    Assert.assertNull(readSelector.idLookup());

    // Value matcher tests.
    if (NullHandling.replaceWithDefault()) {
      Assert.assertTrue(readSelector.makeValueMatcher("0").matches());
      Assert.assertFalse(readSelector.makeValueMatcher((String) null).matches());
      Assert.assertTrue(readSelector.makeValueMatcher("0"::equals).matches());
      Assert.assertFalse(readSelector.makeValueMatcher(Objects::isNull).matches());
    } else {
      Assert.assertFalse(readSelector.makeValueMatcher("0").matches());
      Assert.assertTrue(readSelector.makeValueMatcher((String) null).matches());
      Assert.assertFalse(readSelector.makeValueMatcher("0"::equals).matches());
      Assert.assertTrue(readSelector.makeValueMatcher(Objects::isNull).matches());
    }
  }

  @Test
  public void test_makeDimensionSelector_aValue()
  {
    writeToMemory(5L);

    final DimensionSelector readSelector =
        new LongFieldReader().makeDimensionSelector(memory, new ConstantFieldPointer(MEMORY_POSITION), null);

    // Data retrieval tests.
    final IndexedInts row = readSelector.getRow();
    Assert.assertEquals(1, row.size());
    Assert.assertEquals("5", readSelector.lookupName(0));

    // Informational method tests.
    Assert.assertFalse(readSelector.supportsLookupNameUtf8());
    Assert.assertFalse(readSelector.nameLookupPossibleInAdvance());
    Assert.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, readSelector.getValueCardinality());
    Assert.assertEquals(String.class, readSelector.classOfObject());
    Assert.assertNull(readSelector.idLookup());

    // Value matcher tests.
    Assert.assertTrue(readSelector.makeValueMatcher("5").matches());
    Assert.assertFalse(readSelector.makeValueMatcher("2").matches());
    Assert.assertTrue(readSelector.makeValueMatcher("5"::equals).matches());
    Assert.assertFalse(readSelector.makeValueMatcher("2"::equals).matches());
  }

  @Test
  public void test_makeDimensionSelector_aValue_extractionFn()
  {
    writeToMemory(25L);

    final DimensionSelector readSelector =
        new LongFieldReader().makeDimensionSelector(
            memory,
            new ConstantFieldPointer(MEMORY_POSITION),
            new SubstringDimExtractionFn(1, null)
        );

    // Data retrieval tests.
    final IndexedInts row = readSelector.getRow();
    Assert.assertEquals(1, row.size());
    Assert.assertEquals("5", readSelector.lookupName(0));

    // Informational method tests.
    Assert.assertFalse(readSelector.supportsLookupNameUtf8());
    Assert.assertFalse(readSelector.nameLookupPossibleInAdvance());
    Assert.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, readSelector.getValueCardinality());
    Assert.assertEquals(String.class, readSelector.classOfObject());
    Assert.assertNull(readSelector.idLookup());

    // Value matcher tests.
    Assert.assertTrue(readSelector.makeValueMatcher("5").matches());
    Assert.assertFalse(readSelector.makeValueMatcher("2").matches());
    Assert.assertTrue(readSelector.makeValueMatcher("5"::equals).matches());
    Assert.assertFalse(readSelector.makeValueMatcher("2"::equals).matches());
  }

  private void writeToMemory(final Long value)
  {
    Mockito.when(writeSelector.isNull()).thenReturn(value == null);

    if (value != null) {
      Mockito.when(writeSelector.getLong()).thenReturn(value);
    }

    if (fieldWriter.writeTo(memory, MEMORY_POSITION, memory.getCapacity() - MEMORY_POSITION) < 0) {
      throw new ISE("Could not write");
    }
  }
}
