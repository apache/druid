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
import org.apache.druid.segment.BaseNullableColumnValueSelector;

/**
 * FieldWriter for numeric datatypes. The parent class does the null handling for the underlying data, while
 * the individual subclasses write the individual element (long, float or double type). This also allows for a clean
 * reuse of the readers and writers between the numeric types and also allowing the array writers ({@link NumericArrayFieldWriter})
 * to use these methods directly without duplication
 *
 * Format:
 *  - 1 byte: Whether the following value is null or not. Take a look at the note on the indicator bytes.
 *  - X bytes: Encoded value of the selector, or the default value if it is null. X denotes the size of the numeric value
 *
 * Indicator bytes for denoting whether the element is null or not null changes depending on whether the writer is used
 * to write the data for individual value (like LONG) or for an element of an array (like ARRAY<LONG>). This is because
 * array support for the numeric types was added later and by then the field writers for individual fields were using
 * 0x00 to denote the null byte, which is reserved for denoting the array end when we are writing the elements as part
 * of the array instead. (0x00 is used for array end because it helps in preserving the byte comparison property of the
 * numeric array field writers).
 *
 * Therefore, to preserve backward and forward compatibility, the individual element's writers were left unchanged,
 * while the array's element's writers used 0x01 and 0x02 to denote null and non-null byte respectively
 *
 * Values produced by the writer are sortable without decoding
 *
 * @see NumericArrayFieldWriter for examples of how this class serializes the field for numeric arrays
 */
public abstract class NumericFieldWriter implements FieldWriter
{
  /**
   * Indicator byte denoting that the numeric value succeeding it is null. This is used in the primitive
   * writers. NULL_BYTE < NOT_NULL_BYTE to preserve the ordering while doing byte comparison
   */
  public static final byte NULL_BYTE = 0x00;

  /**
   * Indicator byte denoting that the numeric value succeeding it is not null. This is used in the primitive
   * writers
   */
  public static final byte NOT_NULL_BYTE = 0x01;

  /**
   * Indicator byte denoting that the numeric value succeeding it is null. This is used while writing the individual
   * elements writers of an array. ARRAY_ELEMENT_NULL_BYTE < ARRAY_ELEMENT_NOT_NULL_BYTE to preserve the ordering
   * while doing byte comparison
   */
  public static final byte ARRAY_ELEMENT_NULL_BYTE = 0x01;

  /**
   * Indicator byte denoting that the numeric value succeeding it is not null. This is used while writing the individual
   * elements writers of an array
   */
  public static final byte ARRAY_ELEMENT_NOT_NULL_BYTE = 0x02;

  private final BaseNullableColumnValueSelector selector;
  private final byte nullIndicatorByte;
  private final byte notNullIndicatorByte;

  public NumericFieldWriter(
      final BaseNullableColumnValueSelector selector,
      final boolean forArray
  )
  {
    this.selector = selector;
    if (!forArray) {
      this.nullIndicatorByte = NULL_BYTE;
      this.notNullIndicatorByte = NOT_NULL_BYTE;
    } else {
      this.nullIndicatorByte = ARRAY_ELEMENT_NULL_BYTE;
      this.notNullIndicatorByte = ARRAY_ELEMENT_NOT_NULL_BYTE;
    }
  }

  @Override
  public long writeTo(WritableMemory memory, long position, long maxSize)
  {
    int size = getNumericSizeBytes() + Byte.BYTES;

    if (maxSize < size) {
      return -1;
    }

    // Using isNull() since this is a primitive type
    if (selector.isNull()) {
      memory.putByte(position, nullIndicatorByte);
      writeNullToMemory(memory, position + Byte.BYTES);
    } else {
      memory.putByte(position, notNullIndicatorByte);
      writeSelectorToMemory(memory, position + Byte.BYTES);
    }

    return size;
  }

  @Override
  public void close()
  {
    // Nothing to do
  }

  /**
   * @return The size in bytes of the numeric datatype that the implementation of this writer occupies
   */
  public abstract int getNumericSizeBytes();

  /**
   * Writes the value pointed by the selector to memory. The caller should ensure that the selector gives out the
   * correct primitive type
   */
  public abstract void writeSelectorToMemory(WritableMemory memory, long position);

  /**
   * Writes the default value for the type to the memory. For long, it is 0L, for double, it is 0.0d etc. Useful mainly
   * when the SQL incompatible mode is turned off, and maintains the fact that the size of the numeric field written
   * doesn't vary irrespective of whether the value is null
   */
  public abstract void writeNullToMemory(WritableMemory memory, long position);
}
