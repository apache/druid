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

package org.apache.druid.segment.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

public class NullColumnPartSerdeTest extends InitializedNullHandlingTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();

    final NullColumnPartSerde partSerde = new NullColumnPartSerde(10, new RoaringBitmapSerdeFactory(null));
    final String json = mapper.writeValueAsString(partSerde);
    Assert.assertEquals(partSerde, mapper.readValue(json, ColumnPartSerde.class));
  }

  @Test
  public void testDeserializer()
  {
    final NullColumnPartSerde partSerde = new NullColumnPartSerde(10, new RoaringBitmapSerdeFactory(null));
    final ColumnBuilder builder = new ColumnBuilder().setType(ValueType.DOUBLE);
    partSerde.getDeserializer().read(Mockito.mock(ByteBuffer.class), builder, Mockito.mock(ColumnConfig.class));
    final ColumnCapabilities columnCapabilities = builder.build().getCapabilities();
    Assert.assertTrue(Types.is(columnCapabilities, ValueType.DOUBLE));
    Assert.assertTrue(columnCapabilities.hasNulls().isTrue());
    Assert.assertTrue(columnCapabilities.hasMultipleValues().isFalse());
    Assert.assertTrue(columnCapabilities.isFilterable());
    Assert.assertTrue(columnCapabilities.hasBitmapIndexes());
    Assert.assertTrue(columnCapabilities.isDictionaryEncoded().isTrue());
    Assert.assertTrue(columnCapabilities.areDictionaryValuesSorted().isTrue());
    Assert.assertTrue(columnCapabilities.areDictionaryValuesUnique().isTrue());
  }

  @Test
  public void testDimensionSelector()
  {
    final NullColumnPartSerde partSerde = new NullColumnPartSerde(10, new RoaringBitmapSerdeFactory(null));
    final ColumnBuilder builder = new ColumnBuilder().setType(ValueType.STRING);
    partSerde.getDeserializer().read(Mockito.mock(ByteBuffer.class), builder, Mockito.mock(ColumnConfig.class));
    ColumnHolder holder = builder.build();

    BaseColumn theColumn = holder.getColumn();
    Assert.assertTrue(theColumn instanceof DictionaryEncodedColumn);
    DictionaryEncodedColumn dictionaryEncodedColumn = (DictionaryEncodedColumn) theColumn;

    ReadableOffset offset = new SimpleAscendingOffset(10);
    DimensionSelector dimensionSelector = dictionaryEncodedColumn.makeDimensionSelector(
        offset,
        null
    );
    Assert.assertNull(dimensionSelector.getObject());
    Assert.assertEquals(1, dimensionSelector.getRow().size());
    Assert.assertEquals(0, dimensionSelector.getRow().get(0));
  }

  @Test
  public void testDimensionVectorSelector()
  {
    final NullColumnPartSerde partSerde = new NullColumnPartSerde(10, new RoaringBitmapSerdeFactory(null));
    final ColumnBuilder builder = new ColumnBuilder().setType(ValueType.STRING);
    partSerde.getDeserializer().read(Mockito.mock(ByteBuffer.class), builder, Mockito.mock(ColumnConfig.class));
    ColumnHolder holder = builder.build();

    BaseColumn theColumn = holder.getColumn();
    Assert.assertTrue(theColumn instanceof DictionaryEncodedColumn);
    DictionaryEncodedColumn dictionaryEncodedColumn = (DictionaryEncodedColumn) theColumn;

    ReadableVectorOffset vectorOffset = new NoFilterVectorOffset(8, 0, 10);

    SingleValueDimensionVectorSelector vectorSelector =
        dictionaryEncodedColumn.makeSingleValueDimensionVectorSelector(vectorOffset);

    int[] rowVector = vectorSelector.getRowVector();
    for (int i = 0; i < vectorOffset.getCurrentVectorSize(); i++) {
      Assert.assertEquals(0, rowVector[i]);
      Assert.assertNull(vectorSelector.lookupName(rowVector[i]));
    }

    Assert.assertThrows(UnsupportedOperationException.class, () -> {
      dictionaryEncodedColumn.makeMultiValueDimensionVectorSelector(vectorOffset);
    });
  }

  @Test
  public void testVectorObjectSelector()
  {
    final NullColumnPartSerde partSerde = new NullColumnPartSerde(10, new RoaringBitmapSerdeFactory(null));
    final ColumnBuilder builder = new ColumnBuilder().setType(ValueType.STRING);
    partSerde.getDeserializer().read(Mockito.mock(ByteBuffer.class), builder, Mockito.mock(ColumnConfig.class));
    ColumnHolder holder = builder.build();

    BaseColumn theColumn = holder.getColumn();

    ReadableVectorOffset vectorOffset = new NoFilterVectorOffset(8, 0, 10);

    VectorObjectSelector objectSelector = theColumn.makeVectorObjectSelector(vectorOffset);
    Object[] vector = objectSelector.getObjectVector();
    for (int i = 0; i < vectorOffset.getCurrentVectorSize(); i++) {
      Assert.assertNull(vector[i]);
    }
  }

  @Test
  public void testColumnValueSelector()
  {
    final NullColumnPartSerde partSerde = new NullColumnPartSerde(10, new RoaringBitmapSerdeFactory(null));
    final ColumnBuilder builder = new ColumnBuilder().setType(ValueType.DOUBLE);
    partSerde.getDeserializer().read(Mockito.mock(ByteBuffer.class), builder, Mockito.mock(ColumnConfig.class));
    ColumnHolder holder = builder.build();

    BaseColumn theColumn = holder.getColumn();

    ReadableOffset offset = new SimpleAscendingOffset(10);
    ColumnValueSelector valueSelector = theColumn.makeColumnValueSelector(offset);

    if (NullHandling.sqlCompatible()) {
      Assert.assertTrue(valueSelector.isNull());
    } else {
      Assert.assertFalse(valueSelector.isNull());
    }
    Assert.assertEquals(0.0, valueSelector.getDouble(), 0.0);
  }

  @Test
  public void testVectorValueSelector()
  {
    final NullColumnPartSerde partSerde = new NullColumnPartSerde(10, new RoaringBitmapSerdeFactory(null));
    final ColumnBuilder builder = new ColumnBuilder().setType(ValueType.DOUBLE);
    partSerde.getDeserializer().read(Mockito.mock(ByteBuffer.class), builder, Mockito.mock(ColumnConfig.class));
    ColumnHolder holder = builder.build();

    BaseColumn theColumn = holder.getColumn();
    ReadableVectorOffset vectorOffset = new NoFilterVectorOffset(8, 0, 10);

    VectorValueSelector selector = theColumn.makeVectorValueSelector(vectorOffset);
    double[] vector = selector.getDoubleVector();
    boolean[] nulls = selector.getNullVector();
    for (int i = 0; i < vectorOffset.getCurrentVectorSize(); i++) {
      Assert.assertEquals(0.0, vector[i], 0.0);
      if (NullHandling.sqlCompatible()) {
        Assert.assertTrue(nulls[i]);
      } else {
        Assert.assertFalse(nulls[i]);
      }
    }
  }

  @Test
  public void testIndexSupplier()
  {
    final NullColumnPartSerde partSerde = new NullColumnPartSerde(10, new RoaringBitmapSerdeFactory(null));
    final ColumnBuilder builder = new ColumnBuilder().setType(ValueType.DOUBLE);
    partSerde.getDeserializer().read(Mockito.mock(ByteBuffer.class), builder, Mockito.mock(ColumnConfig.class));
    ColumnHolder holder = builder.build();
    Assert.assertNull(holder.getIndexSupplier());
  }
}
