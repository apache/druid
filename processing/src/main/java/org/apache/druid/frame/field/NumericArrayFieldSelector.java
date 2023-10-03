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

import org.apache.datasketches.memory.Memory;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Base implementation of the column value selector that the concrete numeric field reader implementations inherit from.
 * The selector contains the logic to construct an array written by {@link NumericArrayFieldWriter}, and present it as
 * a column value selector.
 *
 * The inheritors of this class are expected to implement
 *  1. {@link #getIndividualValueAtMemory} Which extracts the element from the field where it was written to. Returns
 *       null if the value at that location represents a null element
 *  2. {@link #getIndividualFieldSize} Which informs the method about the field size corresponding to each element in
 *       the numeric array's serialized representation
 *
 * @param <ElementType> Type of the individual array elements
 */
public abstract class NumericArrayFieldSelector<ElementType extends Number> implements ColumnValueSelector
{
  /**
   * Memory containing the serialized values of the array
   */
  protected final Memory memory;

  /**
   * Pointer to location in the memory. The callers are expected to update the pointer's position to the start of the
   * array that they wish to get prior to {@link #getObject()} call.
   *
   * Frames read and written using {@link org.apache.druid.frame.write.FrameWriter} and
   * {@link org.apache.druid.frame.read.FrameReader} shouldn't worry about this detail, since they automatically update
   * and handle the start location
   */
  private final ReadableFieldPointer fieldPointer;

  /**
   * Position last read, for caching the last fetched result
   */
  private long currentFieldPosition = -1;

  /**
   * Value of the row at the location beginning at {@link #currentFieldPosition}
   */
  private final List<ElementType> currentRow = new ArrayList<>();

  /**
   * Nullity of the row at the location beginning at {@link #currentFieldPosition}
   */
  private boolean currentRowIsNull;

  public NumericArrayFieldSelector(final Memory memory, final ReadableFieldPointer fieldPointer)
  {
    this.memory = memory;
    this.fieldPointer = fieldPointer;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // Do nothing
  }

  @Nullable
  @Override
  public Object getObject()
  {
    final List<ElementType> currentArray = computeCurrentArray();

    if (currentArray == null) {
      return null;
    }

    return currentArray.toArray();
  }

  @Override
  public Class classOfObject()
  {
    return Object.class;
  }

  @Override
  public double getDouble()
  {
    return 0;
  }

  @Override
  public float getFloat()
  {
    return 0;
  }

  @Override
  public long getLong()
  {
    return 0;
  }

  @Override
  public boolean isNull()
  {
    long position = fieldPointer.position();
    final byte firstByte = memory.getByte(position);
    return firstByte == NumericArrayFieldWriter.NULL_ROW;
  }

  /**
   * Returns the value of the individual element written at the given position
   */
  @Nullable
  public abstract ElementType getIndividualValueAtMemory(long position);

  /**
   * Returns the field size that each element in the reader array consumes. It is usually 1 + ElementType.SIZE, to hold
   * the element's nullity, and it's representation.
   */
  public abstract int getIndividualFieldSize();

  @Nullable
  private List<ElementType> computeCurrentArray()
  {
    final long fieldPosition = fieldPointer.position();

    if (fieldPosition != currentFieldPosition) {
      updateCurrentArray(fieldPosition);
    }

    this.currentFieldPosition = fieldPosition;

    if (currentRowIsNull) {
      return null;
    }
    return currentRow;

  }

  private void updateCurrentArray(final long fieldPosition)
  {
    currentRow.clear();
    currentRowIsNull = false;

    long position = fieldPosition;
    long limit = memory.getCapacity();

    // Check the first byte, and if it is null, update the current value to null and return
    if (isNull()) {
      currentRowIsNull = true;
      return;
    }

    // Adding a check here to prevent the position from potentially overflowing
    if (position < limit) {
      position++;
    }

    // Sanity check, to make sure that we see the rowTerminator at the end
    boolean rowTerminatorSeen = false;

    while (position < limit) {
      final byte kind = memory.getByte(position);

      // Break as soon as we see the ARRAY_TERMINATOR (0x00)
      if (kind == NumericArrayFieldWriter.ARRAY_TERMINATOR) {
        rowTerminatorSeen = true;
        break;
      }

      // If terminator not seen, then read the field at that location, and increment the position by the element's field
      // size to read the next element.
      currentRow.add(getIndividualValueAtMemory(position));
      position += getIndividualFieldSize();
    }

    if (!rowTerminatorSeen) {
      throw DruidException.defensive("Unexpected end of field");
    }
  }
}
