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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;
import java.util.Set;

/**
 * Binary serialization for nested field type info, translated into this compact format for storage in segments.
 * The index of the type info here is the same as the field index in {@link CompressedNestedDataComplexColumn#fields}
 */
public class FieldTypeInfo
{
  private static final byte STRING_MASK = 1;
  private static final byte LONG_MASK = 1 << 2;
  private static final byte DOUBLE_MASK = 1 << 3;
  private static final byte STRING_ARRAY_MASK = 1 << 4;
  private static final byte LONG_ARRAY_MASK = 1 << 5;
  private static final byte DOUBLE_ARRAY_MASK = 1 << 6;

  private static final ColumnType[] TYPES = new ColumnType[]{
      ColumnType.STRING,
      null, // mistakes were made...
      ColumnType.LONG,
      ColumnType.DOUBLE,
      ColumnType.STRING_ARRAY,
      ColumnType.LONG_ARRAY,
      ColumnType.DOUBLE_ARRAY
  };

  public static FieldTypeInfo read(ByteBuffer buffer, int length)
  {
    FieldTypeInfo typeInfo = new FieldTypeInfo(buffer);
    buffer.position(buffer.position() + length);
    return typeInfo;
  }

  private final ByteBuffer buffer;
  private final int startOffset;

  public FieldTypeInfo(ByteBuffer buffer)
  {
    this.buffer = buffer;
    this.startOffset = buffer.position();
  }

  public TypeSet getTypes(int fieldIndex)
  {
    return new TypeSet(buffer.get(startOffset + fieldIndex));
  }

  public static class TypeSet
  {
    private final byte types;

    public TypeSet(byte types)
    {
      this.types = types;
    }

    /**
     * If the set contains only a single {@link ColumnType}, return it, else null
     */
    @Nullable
    public ColumnType getSingleType()
    {
      return FieldTypeInfo.getSingleType(types);
    }

    public byte getByteValue()
    {
      return types;
    }

    @Override
    public String toString()
    {
      return convertToSet(types).toString();
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TypeSet typeSet = (TypeSet) o;
      return types == typeSet.types;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(types);
    }
  }

  public static class MutableTypeSet
  {
    private byte types;
    private boolean hasEmptyArray;

    public MutableTypeSet()
    {
      this((byte) 0x00);
    }

    public MutableTypeSet(byte types)
    {
      this.types = types;
    }

    public MutableTypeSet(byte types, boolean hasEmptyArray)
    {
      this.types = types;
      this.hasEmptyArray = hasEmptyArray;
    }

    public MutableTypeSet add(ColumnType type)
    {
      types = FieldTypeInfo.add(types, type);
      return this;
    }

    /**
     * Set a flag when we encounter an empty array or array with only null elements
     */
    public MutableTypeSet addUntypedArray()
    {
      hasEmptyArray = true;
      return this;
    }

    public boolean hasUntypedArray()
    {
      return hasEmptyArray;
    }

    public MutableTypeSet merge(byte other, boolean hasEmptyArray)
    {
      types |= other;
      this.hasEmptyArray = this.hasEmptyArray || hasEmptyArray;
      return this;
    }

    @Nullable
    public ColumnType getSingleType()
    {
      final ColumnType columnType = FieldTypeInfo.getSingleType(types);
      if (hasEmptyArray && columnType != null && !columnType.isArray()) {
        return null;
      }
      // if column only has empty arrays, call it long array
      if (types == 0x00 && hasEmptyArray) {
        return ColumnType.LONG_ARRAY;
      }
      return columnType;
    }

    public boolean isEmpty()
    {
      return types == 0x00;
    }


    public byte getByteValue()
    {
      final ColumnType singleType = FieldTypeInfo.getSingleType(types);
      if (hasEmptyArray && singleType != null && !singleType.isArray()) {
        return FieldTypeInfo.add(types, ColumnType.ofArray(singleType));
      }
      return types;
    }

    @Override
    public String toString()
    {
      return convertToSet(types).toString();
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MutableTypeSet typeSet = (MutableTypeSet) o;
      return types == typeSet.types && hasEmptyArray == typeSet.hasEmptyArray;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(types, hasEmptyArray);
    }
  }

  public static class Writer implements Serializer
  {
    private final SegmentWriteOutMedium segmentWriteOutMedium;
    @Nullable
    private WriteOutBytes valuesOut = null;
    private int numWritten = 0;

    public Writer(SegmentWriteOutMedium segmentWriteOutMedium)
    {
      this.segmentWriteOutMedium = segmentWriteOutMedium;
    }

    public void open() throws IOException
    {
      this.valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
    }

    public void write(MutableTypeSet types) throws IOException
    {
      byte typeByte = types.getByteValue();
      // adjust for empty array if needed
      if (types.hasUntypedArray()) {
        Set<ColumnType> columnTypes = FieldTypeInfo.convertToSet(types.getByteValue());
        ColumnType leastRestrictive = null;
        for (ColumnType type : columnTypes) {
          leastRestrictive = ColumnType.leastRestrictiveType(leastRestrictive, type);
        }
        if (leastRestrictive == null) {
          typeByte = add(typeByte, ColumnType.LONG_ARRAY);
        } else if (!leastRestrictive.isArray()) {
          typeByte = add(typeByte, ColumnType.ofArray(leastRestrictive));
        }
      }
      valuesOut.write(typeByte);
      numWritten++;
    }

    @Override
    public long getSerializedSize()
    {
      return numWritten;
    }

    @Override
    public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
    {
      valuesOut.writeTo(channel);
    }
  }

  @Nullable
  private static ColumnType getSingleType(byte types)
  {
    if (Integer.bitCount(types) == 1) {
      return TYPES[Integer.numberOfTrailingZeros(types)];
    } else {
      return null;
    }
  }

  public static byte add(byte types, ColumnType type)
  {
    switch (type.getType()) {
      case STRING:
        types |= STRING_MASK;
        break;
      case LONG:
        types |= LONG_MASK;
        break;
      case DOUBLE:
        types |= DOUBLE_MASK;
        break;
      case ARRAY:
        Preconditions.checkNotNull(type.getElementType(), "ElementType must not be null");
        switch (type.getElementType().getType()) {
          case STRING:
            types |= STRING_ARRAY_MASK;
            break;
          case LONG:
            types |= LONG_ARRAY_MASK;
            break;
          case DOUBLE:
            types |= DOUBLE_ARRAY_MASK;
            break;
          default:
            throw new ISE("Unsupported nested array type: [%s]", type.asTypeString());
        }
        break;
      default:
        throw new ISE("Unsupported nested type: [%s]", type.asTypeString());
    }
    return types;
  }

  public static Set<ColumnType> convertToSet(byte types)
  {
    final Set<ColumnType> theTypes = Sets.newHashSetWithExpectedSize(4);
    if ((types & STRING_MASK) > 0) {
      theTypes.add(ColumnType.STRING);
    }
    if ((types & LONG_MASK) > 0) {
      theTypes.add(ColumnType.LONG);
    }
    if ((types & DOUBLE_MASK) > 0) {
      theTypes.add(ColumnType.DOUBLE);
    }
    if ((types & STRING_ARRAY_MASK) > 0) {
      theTypes.add(ColumnType.STRING_ARRAY);
    }
    if ((types & DOUBLE_ARRAY_MASK) > 0) {
      theTypes.add(ColumnType.DOUBLE_ARRAY);
    }
    if ((types & LONG_ARRAY_MASK) > 0) {
      theTypes.add(ColumnType.LONG_ARRAY);
    }
    return theTypes;
  }
}
