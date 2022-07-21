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

package org.apache.druid.frame.key;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.frame.field.FieldReader;
import org.apache.druid.frame.field.FieldReaders;
import org.apache.druid.frame.field.RowMemoryFieldPointer;
import org.apache.druid.frame.field.RowReader;
import org.apache.druid.frame.segment.row.ConstantFrameRowPointer;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;

import java.util.ArrayList;
import java.util.List;

/**
 * Embeds the logic to read {@link RowKey} from a particular signature of row-based frame.
 *
 * Stateless and immutable.
 */
public class RowKeyReader
{
  private final RowSignature signature;
  private final RowReader rowReader;

  private RowKeyReader(final RowSignature signature, final RowReader rowReader)
  {
    this.signature = signature;
    this.rowReader = rowReader;
  }

  /**
   * Creates a new {@link RowKeyReader}.
   */
  public static RowKeyReader create(final RowSignature signature)
  {
    final List<FieldReader> fieldReaders = new ArrayList<>(signature.size());

    for (final String columnName : signature.getColumnNames()) {
      final ColumnCapabilities capabilities = signature.getColumnCapabilities(columnName);
      final ColumnType columnType =
          Preconditions.checkNotNull(capabilities, "Type for column [%s]", columnName).toColumnType();

      fieldReaders.add(FieldReaders.create(columnName, columnType));
    }

    return new RowKeyReader(signature, new RowReader(fieldReaders));
  }

  /**
   * Read a value out of a key. This operation is potentially costly and should not be done for every row.
   * Implementations need not be optimized, and may do more work than strictly necessary in order to
   * keep implementation simple.
   */
  public Object read(final RowKey key, final int fieldNumber)
  {
    return rowReader.readField(Memory.wrap(key.array()), 0, key.array().length, fieldNumber);
  }

  /**
   * Read all values out of a key, as a list of objects. This operation is potentially costly and should not be done
   * for every row. Implementations need not be optimized, and may do more work than strictly necessary in order to
   * keep implementation simple.
   */
  public List<Object> read(final RowKey key)
  {
    return rowReader.readRow(Memory.wrap(key.array()), 0, key.array().length);
  }

  /**
   * Determine if a particular field of a particular key has multiple values. Faster than calling
   * {@link #read(RowKey, int)}.
   */
  public boolean hasMultipleValues(final RowKey key, final int fieldNumber)
  {
    // OK to use "get" since our create method guarantees that all fields have types.
    final ColumnType columnType = signature.getColumnType(fieldNumber).get();

    if (columnType.isNumeric() || columnType.is(ValueType.COMPLEX)) {
      // Numeric, complex types can never have multiple values.
      return false;
    } else {
      // Need to check the key.
      final Memory keyMemory = Memory.wrap(key.array());
      final RowMemoryFieldPointer fieldPointer = new RowMemoryFieldPointer(
          keyMemory,
          new ConstantFrameRowPointer(0, keyMemory.getCapacity()),
          fieldNumber,
          rowReader.fieldCount()
      );

      if (columnType.is(ValueType.STRING)) {
        final DimensionSelector selector =
            rowReader.fieldReader(fieldNumber)
                     .makeDimensionSelector(keyMemory, fieldPointer, null);

        return selector.getRow().size() > 1;
      } else {
        final ColumnValueSelector<?> selector =
            rowReader.fieldReader(fieldNumber)
                     .makeColumnValueSelector(keyMemory, fieldPointer);

        return selector.getObject() instanceof List;
      }
    }
  }

  /**
   * Trim a key to a particular fieldCount. The returned key may be a copy, but is not guaranteed to be.
   */
  public RowKey trim(final RowKey key, final int trimmedFieldCount)
  {
    if (trimmedFieldCount == 0) {
      return RowKey.empty();
    } else if (trimmedFieldCount == rowReader.fieldCount()) {
      return key;
    } else {
      if (trimmedFieldCount > rowReader.fieldCount()) {
        throw new IAE("Cannot trim to [%,d] fields, only have [%,d] fields", trimmedFieldCount, rowReader.fieldCount());
      }

      final byte[] keyBytes = key.array();
      final int headerSize = Integer.BYTES * rowReader.fieldCount();
      final int trimmedHeaderSize = Integer.BYTES * trimmedFieldCount;
      final int trimmedFieldsSize = fieldEndPosition(keyBytes, trimmedFieldCount - 1) - headerSize;
      final byte[] trimmedBytes = new byte[trimmedHeaderSize + trimmedFieldsSize];

      // Write new header.
      for (int i = 0; i < trimmedFieldCount; i++) {
        // Adjust positions for new header size.
        final int fieldEndPosition = fieldEndPosition(keyBytes, i) - (headerSize - trimmedHeaderSize);
        trimmedBytes[Integer.BYTES * i] = (byte) fieldEndPosition;
        trimmedBytes[Integer.BYTES * i + 1] = (byte) (fieldEndPosition >> 8);
        trimmedBytes[Integer.BYTES * i + 2] = (byte) (fieldEndPosition >> 16);
        trimmedBytes[Integer.BYTES * i + 3] = (byte) (fieldEndPosition >> 24);
      }

      // Write fields.
      System.arraycopy(keyBytes, headerSize, trimmedBytes, trimmedHeaderSize, trimmedFieldsSize);

      return RowKey.wrap(trimmedBytes);
    }
  }

  public static int fieldEndPosition(final byte[] keyBytes, final int fieldNumber)
  {
    return Ints.fromBytes(
        keyBytes[fieldNumber * Integer.BYTES + 3],
        keyBytes[fieldNumber * Integer.BYTES + 2],
        keyBytes[fieldNumber * Integer.BYTES + 1],
        keyBytes[fieldNumber * Integer.BYTES]
    );
  }
}
