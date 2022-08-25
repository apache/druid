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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class ComplexFieldReaderTest extends InitializedNullHandlingTest
{
  private static final ComplexMetricSerde SERDE = new StringComplexMetricSerde();
  private static final long MEMORY_POSITION = 1;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  public BaseObjectColumnValueSelector<String> writeSelector;

  private WritableMemory memory;
  private FieldWriter fieldWriter;

  @Before
  public void setUp()
  {
    memory = WritableMemory.allocate(1000);
    fieldWriter = new ComplexFieldWriter(SERDE, writeSelector);
  }

  @After
  public void tearDown()
  {
    fieldWriter.close();
  }

  @Test
  public void test_createFromType_notComplex()
  {
    final IllegalStateException e = Assert.assertThrows(
        IllegalStateException.class,
        () -> ComplexFieldReader.createFromType(ColumnType.LONG)
    );

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Expected complex type"))
    );
  }

  @Test
  public void test_createFromType_noComplexSerde()
  {
    final IllegalStateException e = Assert.assertThrows(
        IllegalStateException.class,
        () -> ComplexFieldReader.createFromType(ColumnType.ofComplex("no-serde"))
    );

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("No serde for complexTypeName[no-serde]"))
    );
  }

  @Test
  public void test_makeColumnValueSelector_null()
  {
    writeToMemory(null);

    final ColumnValueSelector<?> readSelector =
        new ComplexFieldReader(SERDE).makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION));

    Assert.assertNull(readSelector.getObject());
  }

  @Test
  public void test_makeColumnValueSelector_aValue()
  {
    writeToMemory("foo");

    final ColumnValueSelector<?> readSelector =
        new ComplexFieldReader(SERDE).makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION));

    Assert.assertEquals("foo", readSelector.getObject());
  }

  private void writeToMemory(final String value)
  {
    Mockito.when(writeSelector.getObject()).thenReturn(value);

    if (fieldWriter.writeTo(memory, MEMORY_POSITION, memory.getCapacity() - MEMORY_POSITION) < 0) {
      throw new ISE("Could not write");
    }
  }

  private static class StringComplexMetricSerde extends ComplexMetricSerde
  {
    @Override
    public String getTypeName()
    {
      return "testString";
    }

    @Override
    public ComplexMetricExtractor getExtractor()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public ObjectStrategy getObjectStrategy()
    {
      return new ObjectStrategy<String>()
      {
        @Override
        public Class<String> getClazz()
        {
          return String.class;
        }

        @Override
        public String fromByteBuffer(ByteBuffer buffer, int numBytes)
        {
          return StringUtils.fromUtf8(buffer, numBytes);
        }

        @Override
        public byte[] toBytes(@Nullable String val)
        {
          return StringUtils.toUtf8WithNullToEmpty(val);
        }

        @Override
        public int compare(String o1, String o2)
        {
          return Comparators.<String>naturalNullsFirst().compare(o1, o2);
        }
      };
    }
  }
}
