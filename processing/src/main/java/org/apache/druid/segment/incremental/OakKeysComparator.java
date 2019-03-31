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

package org.apache.druid.segment.incremental;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.oath.oak.OakComparator;
import org.apache.druid.segment.DimensionIndexer;


import java.nio.ByteBuffer;
import java.util.List;

public class OakKeysComparator implements OakComparator<IncrementalIndexRow>
{

  private final List<IncrementalIndex.DimensionDesc> dimensionDescsList;
  private final boolean rollup;

  public OakKeysComparator(List<IncrementalIndex.DimensionDesc> dimensionDescsList, boolean rollup)
  {
    this.dimensionDescsList = dimensionDescsList;
    this.rollup = rollup;
  }

  @Override
  public int compareKeys(IncrementalIndexRow lhs, IncrementalIndexRow rhs)
  {
    int retVal = Longs.compare(lhs.getTimestamp(), rhs.getTimestamp());
    int lhsDimsLength = lhs.getDimsLength();
    int rhsDimsLength = rhs.getDimsLength();
    int numComparisons = Math.min(lhsDimsLength, rhsDimsLength);

    int index = 0;
    while (retVal == 0 && index < numComparisons) {
      final Object lhsIdxs = lhs.getDim(index);
      final Object rhsIdxs = rhs.getDim(index);

      if (lhsIdxs == null) {
        if (rhsIdxs == null) {
          ++index;
          continue;
        }
        return -1;
      }

      if (rhsIdxs == null) {
        return 1;
      }

      final DimensionIndexer indexer = dimensionDescsList.get(index).getIndexer();
      retVal = indexer.compareUnsortedEncodedKeyComponents(lhsIdxs, rhsIdxs);
      ++index;
    }
    if (retVal == 0) {
      int lengthDiff = Ints.compare(lhsDimsLength, rhsDimsLength);
      if (lengthDiff == 0) {
        return lastCompare(lhs.getRowIndex(), rhs.getRowIndex());
      }
      if (lengthDiff > 0) {
        // lhs has bigger dims
        if (allNull(lhs, numComparisons)) {
          return lastCompare(lhs.getRowIndex(), rhs.getRowIndex());
        }
      } else {
        // rhs has bigger dims
        if (allNull(rhs, numComparisons)) {
          return lastCompare(lhs.getRowIndex(), rhs.getRowIndex());
        }
      }
      return lengthDiff;
    }
    return retVal;
  }

  @Override
  public int compareSerializedKeys(ByteBuffer lhs, ByteBuffer rhs)
  {
    int retVal = Longs.compare(OakUtils.getTimestamp(lhs), OakUtils.getTimestamp(rhs));
    int lhsDimsLength = OakUtils.getDimsLength(lhs);
    int rhsDimsLength = OakUtils.getDimsLength(rhs);
    int numComparisons = Math.min(lhsDimsLength, rhsDimsLength);

    int dimIndex = 0;
    while (retVal == 0 && dimIndex < numComparisons) {
      int lhsType = lhs.getInt(OakUtils.getDimIndexInBuffer(lhs, lhsDimsLength, dimIndex));
      int rhsType = rhs.getInt(OakUtils.getDimIndexInBuffer(rhs, rhsDimsLength, dimIndex));

      if (lhsType == OakUtils.NO_DIM) {
        if (rhsType == OakUtils.NO_DIM) {
          ++dimIndex;
          continue;
        }
        return -1;
      }

      if (rhsType == OakUtils.NO_DIM) {
        return 1;
      }

      final DimensionIndexer indexer = dimensionDescsList.get(dimIndex).getIndexer();
      Object lhsObject = OakUtils.getDimValue(lhs, dimIndex);
      Object rhsObject = OakUtils.getDimValue(rhs, dimIndex);
      retVal = indexer.compareUnsortedEncodedKeyComponents(lhsObject, rhsObject);
      ++dimIndex;
    }

    if (retVal == 0) {
      int lengthDiff = Ints.compare(lhsDimsLength, rhsDimsLength);
      if (lengthDiff == 0) {
        return lastCompare(OakUtils.getRowIndex(lhs), OakUtils.getRowIndex(rhs));
      }
      if (lengthDiff > 0) {
        // lhs has bigger dims
        if (OakUtils.checkDimsAllNull(lhs, numComparisons)) {
          return lastCompare(OakUtils.getRowIndex(lhs), OakUtils.getRowIndex(rhs));
        }
      } else {
        // rhs has bigger dims
        if (OakUtils.checkDimsAllNull(rhs, numComparisons)) {
          return lastCompare(OakUtils.getRowIndex(lhs), OakUtils.getRowIndex(rhs));
        }
      }
      return lengthDiff;
    }

    return retVal;
  }

  private int lastCompare(int lsIndex, int rsIndex)
  {
    if (!rollup || lsIndex == IncrementalIndexRow.EMPTY_ROW_INDEX || rsIndex == IncrementalIndexRow.EMPTY_ROW_INDEX) {
      // If we are not rollup then keys shouldnt collide.
      // If one of the keys is EMPTY_ROW_INDEX this is a lower or upper bound key and must be compared.
      return lsIndex - rsIndex;
    } else {
      return 0;
    }
  }


  @Override
  public int compareSerializedKeyAndKey(ByteBuffer lhs, IncrementalIndexRow rhs)
  {
    int retVal = Longs.compare(OakUtils.getTimestamp(lhs), rhs.getTimestamp());
    int lhsDimsLength = OakUtils.getDimsLength(lhs);
    int rhsDimsLength = rhs.getDimsLength();
    int numComparisons = Math.min(lhsDimsLength, rhsDimsLength);

    int index = 0;
    while (retVal == 0 && index < numComparisons) {
      final Object lhsIdxs = OakUtils.getDimValue(lhs, index);
      final Object rhsIdxs = rhs.getDim(index);

      if (lhsIdxs == null) {
        if (rhsIdxs == null) {
          ++index;
          continue;
        }
        return -1;
      }

      if (rhsIdxs == null) {
        return 1;
      }

      final DimensionIndexer indexer = dimensionDescsList.get(index).getIndexer();
      retVal = indexer.compareUnsortedEncodedKeyComponents(lhsIdxs, rhsIdxs);
      ++index;
    }

    if (retVal == 0) {
      int lengthDiff = Ints.compare(lhsDimsLength, rhsDimsLength);
      if (lengthDiff == 0) {
        return lastCompare(OakUtils.getRowIndex(lhs), rhs.getRowIndex());
      }
      if (lengthDiff > 0) {
        // lhs has bigger dims
        if (OakUtils.checkDimsAllNull(lhs, numComparisons)) {
          return lastCompare(OakUtils.getRowIndex(lhs), rhs.getRowIndex());
        }
      } else {
        // rhs has bigger dims
        if (allNull(rhs, numComparisons)) {
          return lastCompare(OakUtils.getRowIndex(lhs), rhs.getRowIndex());
        }
      }
      return lengthDiff;
    }
    return retVal;
  }

  private static boolean allNull(IncrementalIndexRow row, int startPosition)
  {
    for (int i = startPosition; i < row.getDimsLength(); i++) {
      if (row.getDim(i) != null) {
        return false;
      }
    }
    return true;
  }


}
