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

package org.apache.druid.query.rowsandcols.column;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.util.FindResult;

public interface BinarySearchableAccessor extends ColumnAccessor
{
  static BinarySearchableAccessor fromColumn(Column col)
  {
    final BinarySearchableAccessor retVal = col.as(BinarySearchableAccessor.class);

    if (retVal == null) {
      final ColumnAccessor accessor = col.toAccessor();
      if (accessor instanceof BinarySearchableAccessor) {
        // Why didn't they just return it from the as()!?!?!  Who knows, ah well.
        return (BinarySearchableAccessor) accessor;
      }

      throw new ISE("col[%s] is a no-go", col.getClass());
    }
    return retVal;
  }

  FindResult findNull(int startIndex, int endIndex);

  FindResult findDouble(int startIndex, int endIndex, double val);

  FindResult findFloat(int startIndex, int endIndex, float val);

  FindResult findLong(int startIndex, int endIndex, long val);

  FindResult findString(int startIndex, int endIndex, String val);

  FindResult findComplex(int startIndex, int endIndex, Object val);
}
