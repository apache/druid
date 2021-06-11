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

package org.apache.druid.query.filter.vector;

import com.google.common.base.Preconditions;
import org.apache.druid.segment.QueryableIndexStorageAdapter;

import javax.annotation.Nullable;

/**
 * Implementation class for ReadableVectorMatch.
 *
 * Also adds some useful methods, like "addAll", "removeAll", and "copyFrom".
 */
public class VectorMatch implements ReadableVectorMatch
{
  private static final int[] DEFAULT_ALL_TRUE_VECTOR = new int[QueryableIndexStorageAdapter.DEFAULT_VECTOR_SIZE];

  private static final VectorMatch ALL_FALSE = new VectorMatch(new int[0], 0);

  static {
    for (int i = 0; i < DEFAULT_ALL_TRUE_VECTOR.length; i++) {
      DEFAULT_ALL_TRUE_VECTOR[i] = i;
    }
  }

  private final int[] selection;
  private int selectionSize;

  private VectorMatch(final int[] selection, final int selectionSize)
  {
    this.selection = selection;
    this.selectionSize = selectionSize;
  }

  /**
   * Creates a match that matches everything up to "numRows". This will often be the current vector size, but
   * does not necessarily have to be.
   */
  public static ReadableVectorMatch allTrue(final int numRows)
  {
    if (numRows <= DEFAULT_ALL_TRUE_VECTOR.length) {
      return new VectorMatch(DEFAULT_ALL_TRUE_VECTOR, numRows);
    } else {
      final int[] selection = new int[numRows];

      for (int i = 0; i < numRows; i++) {
        selection[i] = i;
      }

      return new VectorMatch(selection, numRows);
    }
  }

  /**
   * Creates a match that matches nothing.
   */
  public static ReadableVectorMatch allFalse()
  {
    return ALL_FALSE;
  }

  /**
   * Creates a new match object with selectionSize = 0, and the provided array as a backing array.
   */
  public static VectorMatch wrap(final int[] selection)
  {
    return new VectorMatch(selection, 0);
  }

  @Override
  public boolean isAllTrue(final int vectorSize)
  {
    return selectionSize == vectorSize;
  }

  @Override
  public boolean isAllFalse()
  {
    return selectionSize == 0;
  }

  @Override
  public boolean isValid(@Nullable final ReadableVectorMatch mask)
  {
    if (mask != null && !mask.isValid(null)) {
      // Invalid mask.
      return false;
    }

    // row numbers must be increasing.
    int rowNum = -1;
    for (int i = 0; i < selectionSize; i++) {
      if (selection[i] > rowNum) {
        rowNum = selection[i];
      } else {
        return false;
      }
    }

    // row number cannot be larger than the max length of the selection vector.
    if (rowNum > selection.length) {
      return false;
    }

    // row numbers must all be present in the mask, if it exists.
    if (mask != null) {
      final int[] maskArray = mask.getSelection();
      for (int i = 0, j = 0; i < selectionSize; i++) {
        while (j < mask.getSelectionSize() && selection[i] > maskArray[j]) {
          j++;
        }

        if (j >= mask.getSelectionSize() || selection[i] != maskArray[j]) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Removes all rows from this object that occur in "other", in place, and returns a reference to this object. Does
   * not modify "other".
   *
   * "other" cannot be the same instance as this object.
   */
  public VectorMatch removeAll(final ReadableVectorMatch other)
  {
    //noinspection ObjectEquality
    Preconditions.checkState(this != other, "'other' must be a different instance from 'this'");

    int i = 0; // reading position in this.selection
    int j = 0; // writing position in this.selection
    int p = 0; // position in otherSelection
    final int[] otherSelection = other.getSelection();
    for (; i < selectionSize; i++) {
      while (p < other.getSelectionSize() && otherSelection[p] < selection[i]) {
        // Other value < selection[i], keep reading in other so we can see if selection[i] should be preserved or not.
        p++;
      }

      if (!(p < other.getSelectionSize() && otherSelection[p] == selection[i])) {
        // Preserve selection[i].
        selection[j++] = selection[i];
      }
    }
    selectionSize = j;
    assert isValid(null);
    return this;
  }

  /**
   * Adds all rows from "other" to this object, using "scratch" as scratch space if needed. Does not modify "other".
   * Returns a reference to this object.
   *
   * "other" and "scratch" cannot be the same instance as each other, or as this object.
   */
  public VectorMatch addAll(final ReadableVectorMatch other, final VectorMatch scratch)
  {
    //noinspection ObjectEquality
    Preconditions.checkState(this != scratch, "'scratch' must be a different instance from 'this'");
    //noinspection ObjectEquality
    Preconditions.checkState(other != scratch, "'scratch' must be a different instance from 'other'");
    //noinspection ObjectEquality
    Preconditions.checkState(this != other, "'other' must be a different instance from 'this'");

    final int[] scratchSelection = scratch.getSelection();
    final int[] otherSelection = other.getSelection();

    int i = 0; // this.selection pointer
    int j = 0; // otherSelection pointer
    int k = 0; // scratchSelection pointer

    for (; i < selectionSize; i++) {
      while (j < other.getSelectionSize() && otherSelection[j] < selection[i]) {
        scratchSelection[k++] = otherSelection[j++];
      }

      scratchSelection[k++] = selection[i];

      if (j < other.getSelectionSize() && otherSelection[j] == selection[i]) {
        j++;
      }
    }

    while (j < other.getSelectionSize()) {
      scratchSelection[k++] = otherSelection[j++];
    }

    scratch.setSelectionSize(k);
    copyFrom(scratch);
    assert isValid(null);
    return this;
  }

  /**
   * Copies "other" into this object, and returns a reference to this object. Does not modify "other".
   *
   * "other" cannot be the same instance as this object.
   */
  public void copyFrom(final ReadableVectorMatch other)
  {
    //noinspection ObjectEquality
    Preconditions.checkState(this != other, "'other' must be a different instance from 'this'");

    Preconditions.checkState(
        selection.length >= other.getSelectionSize(),
        "Capacity[%s] cannot fit other match's selectionSize[%s]",
        selection.length,
        other.getSelectionSize()
    );

    System.arraycopy(other.getSelection(), 0, selection, 0, other.getSelectionSize());
    selectionSize = other.getSelectionSize();
    assert isValid(null);
  }

  @Override
  public int[] getSelection()
  {
    return selection;
  }

  @Override
  public int getSelectionSize()
  {
    return selectionSize;
  }

  /**
   * Sets the valid selectionSize, and returns a reference to this object.
   */
  public VectorMatch setSelectionSize(final int newSelectionSize)
  {
    Preconditions.checkArgument(
        newSelectionSize <= selection.length,
        "Oops! Cannot setSelectionSize[%s] > selection.length[%s].",
        newSelectionSize,
        selection.length
    );
    this.selectionSize = newSelectionSize;
    assert isValid(null);
    return this;
  }

  @Override
  public String toString()
  {
    final StringBuilder retVal = new StringBuilder("[");
    for (int i = 0; i < selectionSize; i++) {
      if (i > 0) {
        retVal.append(", ");
      }
      retVal.append(selection[i]);
    }
    retVal.append("]");
    return retVal.toString();
  }
}
