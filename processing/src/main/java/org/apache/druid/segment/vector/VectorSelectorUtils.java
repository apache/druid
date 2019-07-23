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

package org.apache.druid.segment.vector;

import org.apache.druid.collections.bitmap.ImmutableBitmap;

import javax.annotation.Nullable;

public class VectorSelectorUtils
{
  /**
   * Helper used by ColumnarLongs, ColumnarDoubles, etc. to populate null-flag vectors.
   */
  @Nullable
  public static boolean[] populateNullVector(
      @Nullable final boolean[] nullVector,
      final ReadableVectorOffset offset,
      final ImmutableBitmap nullValueBitmap
  )
  {
    if (nullValueBitmap.isEmpty()) {
      return null;
    }

    final boolean[] retVal;

    if (nullVector != null) {
      retVal = nullVector;
    } else {
      retVal = new boolean[offset.getMaxVectorSize()];
    }

    // Probably not super efficient to call "get" so much, but, no worse than the non-vectorized version.
    if (offset.isContiguous()) {
      for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
        retVal[i] = nullValueBitmap.get(i + offset.getStartOffset());
      }
    } else {
      for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
        retVal[i] = nullValueBitmap.get(offset.getOffsets()[i]);
      }
    }

    return retVal;
  }
}
