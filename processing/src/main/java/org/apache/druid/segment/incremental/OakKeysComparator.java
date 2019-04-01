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


  private int compare(IncrementalIndexRow lhs, IncrementalIndexRow rhs)
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
  public int compareKeys(IncrementalIndexRow lhs, IncrementalIndexRow rhs)
  {
    return compare(lhs, rhs);
  }

  @Override
  public int compareSerializedKeys(ByteBuffer lhs, ByteBuffer rhs)
  {
    return compare(new OakIncrementalIndexRow(lhs, null, null),
            new OakIncrementalIndexRow(rhs, null, null));

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
    return compare(new OakIncrementalIndexRow(lhs, null, null), rhs);
  }

  private static boolean allNull(IncrementalIndexRow row, int startPosition)
  {
    for (int i = startPosition; i < row.getDimsLength(); i++) {
      if (!row.isDimNull(i)) {
        return false;
      }
    }
    return true;
  }
}
