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
import org.apache.druid.segment.DimensionIndexer;
import com.oath.oak.OakComparator;

import java.nio.ByteBuffer;
import java.util.List;

public class OakKeysComparator implements OakComparator<IncrementalIndexRow>
{

  private List<IncrementalIndex.DimensionDesc> dimensionDescsList;
  private boolean rollup;

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
        if (rollup) {
          return 0;
        }
        return lhs.getRowIndex() - rhs.getRowIndex();
      }
      IncrementalIndexRow largerDims = lengthDiff > 0 ? lhs : rhs;
      if (IncrementalIndex.allNull(largerDims, numComparisons)) {
        if (rollup) {
          return 0;
        }
        return lhs.getRowIndex() - rhs.getRowIndex();
      }
      return lengthDiff;
    }

    return retVal;
  }

  @Override
  public int compareSerializedKeys(ByteBuffer lhs, ByteBuffer rhs)
  {
    int retVal = Longs.compare(OakIncrementalIndex.getTimestamp(lhs), OakIncrementalIndex.getTimestamp(rhs));
    int lhsDimsLength = OakIncrementalIndex.getDimsLength(lhs);
    int rhsDimsLength = OakIncrementalIndex.getDimsLength(rhs);
    int numComparisons = Math.min(lhsDimsLength, rhsDimsLength);

    int dimIndex = 0;
    while (retVal == 0 && dimIndex < numComparisons) {
      int lhsType = lhs.getInt(OakIncrementalIndex.getDimIndexInBuffer(lhs, lhsDimsLength, dimIndex));
      int rhsType = rhs.getInt(OakIncrementalIndex.getDimIndexInBuffer(rhs, rhsDimsLength, dimIndex));

      if (lhsType == OakIncrementalIndex.NO_DIM) {
        if (rhsType == OakIncrementalIndex.NO_DIM) {
          ++dimIndex;
          continue;
        }
        return -1;
      }

      if (rhsType == OakIncrementalIndex.NO_DIM) {
        return 1;
      }

      final DimensionIndexer indexer = dimensionDescsList.get(dimIndex).getIndexer();
      Object lhsObject = OakIncrementalIndex.getDimValue(lhs, dimIndex);
      Object rhsObject = OakIncrementalIndex.getDimValue(rhs, dimIndex);
      retVal = indexer.compareUnsortedEncodedKeyComponents(lhsObject, rhsObject);
      ++dimIndex;
    }

    if (retVal == 0) {
      int lengthDiff = Ints.compare(lhsDimsLength, rhsDimsLength);
      if (lengthDiff == 0) {
        if (rollup) {
          return 0;
        }
        return OakIncrementalIndex.getRowIndex(lhs) - OakIncrementalIndex.getRowIndex(rhs);
      }
      ByteBuffer largerDims = lengthDiff > 0 ? lhs : rhs;
      if (OakIncrementalIndex.checkDimsAllNull(largerDims, numComparisons)) {
        if (rollup) {
          return 0;
        }
        return OakIncrementalIndex.getRowIndex(lhs) - OakIncrementalIndex.getRowIndex(rhs);
      }
      return lengthDiff;
    }
    return retVal;
  }

  @Override
  public int compareSerializedKeyAndKey(ByteBuffer lhs, IncrementalIndexRow rhs)
  {
    int retVal = Longs.compare(OakIncrementalIndex.getTimestamp(lhs), rhs.getTimestamp());
    int lhsDimsLength = OakIncrementalIndex.getDimsLength(lhs);
    int rhsDimsLength = rhs.getDimsLength();
    int numComparisons = Math.min(lhsDimsLength, rhsDimsLength);

    int index = 0;
    while (retVal == 0 && index < numComparisons) {
      final Object lhsIdxs = OakIncrementalIndex.getDimValue(lhs, index);
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
        if (rollup) {
          return 0;
        }
        return OakIncrementalIndex.getRowIndex(lhs) - rhs.getRowIndex();
      }

      if (lengthDiff > 0) {
        if (OakIncrementalIndex.checkDimsAllNull(lhs, numComparisons)) {
          if (rollup) {
            return 0;
          }
          return OakIncrementalIndex.getRowIndex(lhs) - rhs.getRowIndex();
        }
      } else {
        if (OakIncrementalIndex.allNull(rhs, numComparisons)) {
          if (rollup) {
            return 0;
          }
          return OakIncrementalIndex.getRowIndex(lhs) - rhs.getRowIndex();
        }
      }
      return lengthDiff;
    }

    return retVal;
  }
}
