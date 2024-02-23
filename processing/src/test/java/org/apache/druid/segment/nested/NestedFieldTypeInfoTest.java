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

package org.apache.druid.segment.nested;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Set;

public class NestedFieldTypeInfoTest
{
  private static final ByteBuffer BUFFER = ByteBuffer.allocate(1024).order(ByteOrder.nativeOrder());

  @Test
  public void testSingleType() throws IOException
  {
    List<ColumnType> supportedTypes = ImmutableList.of(
        ColumnType.STRING,
        ColumnType.LONG,
        ColumnType.DOUBLE,
        ColumnType.STRING_ARRAY,
        ColumnType.LONG_ARRAY,
        ColumnType.DOUBLE_ARRAY
    );

    for (ColumnType type : supportedTypes) {
      testSingleType(type);
    }
  }

  @Test
  public void testSingleTypeWithEmptyArray() throws IOException
  {
    List<ColumnType> supportedTypes = ImmutableList.of(
        ColumnType.STRING,
        ColumnType.LONG,
        ColumnType.DOUBLE,
        ColumnType.STRING_ARRAY,
        ColumnType.LONG_ARRAY,
        ColumnType.DOUBLE_ARRAY
    );

    for (ColumnType type : supportedTypes) {
      testSingleTypeWithEmptyArray(type);
    }
  }

  @Test
  public void testMultiType() throws IOException
  {
    List<Set<ColumnType>> tests = ImmutableList.of(
        ImmutableSet.of(ColumnType.STRING, ColumnType.LONG),
        ImmutableSet.of(ColumnType.LONG, ColumnType.DOUBLE),
        ImmutableSet.of(ColumnType.STRING, ColumnType.LONG, ColumnType.DOUBLE),
        ImmutableSet.of(ColumnType.DOUBLE, ColumnType.DOUBLE_ARRAY),
        ImmutableSet.of(ColumnType.LONG_ARRAY, ColumnType.DOUBLE_ARRAY)
    );

    for (Set<ColumnType> typeSet : tests) {
      testMultiType(typeSet);
    }
  }

  @Test
  public void testOnlyEmptyType()
  {
    FieldTypeInfo.MutableTypeSet typeSet = new FieldTypeInfo.MutableTypeSet();
    Assert.assertNull(typeSet.getSingleType());
    Assert.assertTrue(typeSet.isEmpty());

    typeSet.addUntypedArray();

    Assert.assertEquals(ColumnType.LONG_ARRAY, typeSet.getSingleType());
    // no actual types in the type set, only getSingleType
    Assert.assertEquals(ImmutableSet.of(), FieldTypeInfo.convertToSet(typeSet.getByteValue()));
    Assert.assertTrue(typeSet.hasUntypedArray());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(FieldTypeInfo.TypeSet.class)
                  .usingGetClass()
                  .verify();

    EqualsVerifier.forClass(FieldTypeInfo.MutableTypeSet.class)
                  .suppress(Warning.NONFINAL_FIELDS)
                  .usingGetClass()
                  .verify();
  }

  private void testSingleType(ColumnType columnType) throws IOException
  {
    FieldTypeInfo.MutableTypeSet typeSet = new FieldTypeInfo.MutableTypeSet();
    Assert.assertNull(typeSet.getSingleType());
    Assert.assertTrue(typeSet.isEmpty());

    typeSet.add(columnType);

    Assert.assertEquals(columnType, typeSet.getSingleType());
    Assert.assertEquals(ImmutableSet.of(columnType), FieldTypeInfo.convertToSet(typeSet.getByteValue()));

    writeTypeSet(typeSet);
    FieldTypeInfo info = new FieldTypeInfo(BUFFER);
    Assert.assertEquals(0, BUFFER.position());

    FieldTypeInfo.TypeSet roundTrip = info.getTypes(0);
    Assert.assertEquals(columnType, roundTrip.getSingleType());

    FieldTypeInfo info2 = FieldTypeInfo.read(BUFFER, 1);
    Assert.assertEquals(info.getTypes(0), info2.getTypes(0));
    Assert.assertEquals(1, BUFFER.position());
  }

  private void testMultiType(Set<ColumnType> columnTypes) throws IOException
  {
    FieldTypeInfo.MutableTypeSet typeSet = new FieldTypeInfo.MutableTypeSet();
    Assert.assertNull(typeSet.getSingleType());
    Assert.assertTrue(typeSet.isEmpty());

    FieldTypeInfo.MutableTypeSet merge = new FieldTypeInfo.MutableTypeSet();
    for (ColumnType columnType : columnTypes) {
      typeSet.add(columnType);
      merge.merge(new FieldTypeInfo.MutableTypeSet().add(columnType).getByteValue(), false);
    }

    Assert.assertEquals(merge.getByteValue(), typeSet.getByteValue());
    Assert.assertNull(typeSet.getSingleType());
    Assert.assertEquals(columnTypes, FieldTypeInfo.convertToSet(typeSet.getByteValue()));

    writeTypeSet(typeSet);
    FieldTypeInfo info = new FieldTypeInfo(BUFFER);
    Assert.assertEquals(0, BUFFER.position());

    FieldTypeInfo.TypeSet roundTrip = info.getTypes(0);
    Assert.assertNull(roundTrip.getSingleType());
    Assert.assertEquals(columnTypes, FieldTypeInfo.convertToSet(roundTrip.getByteValue()));

    FieldTypeInfo info2 = FieldTypeInfo.read(BUFFER, 1);
    Assert.assertEquals(info.getTypes(0), info2.getTypes(0));
    Assert.assertEquals(1, BUFFER.position());
  }

  private void testSingleTypeWithEmptyArray(ColumnType columnType) throws IOException
  {
    FieldTypeInfo.MutableTypeSet typeSet = new FieldTypeInfo.MutableTypeSet();
    typeSet.add(columnType);
    typeSet.addUntypedArray();

    if (columnType.isArray()) {
      // arrays with empty arrays are still single type
      Assert.assertEquals(columnType, typeSet.getSingleType());
      Assert.assertEquals(ImmutableSet.of(columnType), FieldTypeInfo.convertToSet(typeSet.getByteValue()));

      writeTypeSet(typeSet);
      FieldTypeInfo info = new FieldTypeInfo(BUFFER);
      Assert.assertEquals(0, BUFFER.position());

      FieldTypeInfo.TypeSet roundTrip = info.getTypes(0);
      Assert.assertEquals(columnType, roundTrip.getSingleType());

      FieldTypeInfo info2 = FieldTypeInfo.read(BUFFER, 1);
      Assert.assertEquals(info.getTypes(0), info2.getTypes(0));
      Assert.assertEquals(1, BUFFER.position());
    } else {
      // scalar types become multi-type
      Set<ColumnType> columnTypes = ImmutableSet.of(columnType, ColumnType.ofArray(columnType));
      FieldTypeInfo.MutableTypeSet merge = new FieldTypeInfo.MutableTypeSet();
      merge.merge(new FieldTypeInfo.MutableTypeSet().add(columnType).getByteValue(), true);

      Assert.assertEquals(merge.getByteValue(), typeSet.getByteValue());
      Assert.assertNull(typeSet.getSingleType());
      Assert.assertEquals(columnTypes, FieldTypeInfo.convertToSet(typeSet.getByteValue()));

      writeTypeSet(typeSet);
      FieldTypeInfo info = new FieldTypeInfo(BUFFER);
      Assert.assertEquals(0, BUFFER.position());

      FieldTypeInfo.TypeSet roundTrip = info.getTypes(0);
      Assert.assertNull(roundTrip.getSingleType());
      Assert.assertEquals(columnTypes, FieldTypeInfo.convertToSet(roundTrip.getByteValue()));

      FieldTypeInfo info2 = FieldTypeInfo.read(BUFFER, 1);
      Assert.assertEquals(info.getTypes(0), info2.getTypes(0));
      Assert.assertEquals(1, BUFFER.position());
    }
  }

  private static void writeTypeSet(FieldTypeInfo.MutableTypeSet typeSet) throws IOException
  {
    BUFFER.position(0);
    FieldTypeInfo.Writer writer = new FieldTypeInfo.Writer(new OnHeapMemorySegmentWriteOutMedium());
    writer.open();
    writer.write(typeSet);
    Assert.assertEquals(1, writer.getSerializedSize());

    WritableByteChannel channel = new WritableByteChannel()
    {
      @Override
      public int write(ByteBuffer src)
      {
        int size = src.remaining();
        BUFFER.put(src);
        return size;
      }

      @Override
      public boolean isOpen()
      {
        return true;
      }

      @Override
      public void close()
      {
      }
    };
    writer.writeTo(channel, null);
    Assert.assertEquals(1, BUFFER.position());

    BUFFER.position(0);
  }
}
