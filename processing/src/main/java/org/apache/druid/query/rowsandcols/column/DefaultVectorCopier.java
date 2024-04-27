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

public class DefaultVectorCopier implements VectorCopier
{
  ColumnAccessor accessor;

  public DefaultVectorCopier(ColumnAccessor accessor)
  {
    this.accessor = accessor;
  }

  @Override
  public void copyInto(Object[] into, int intoStart)
  {
    final int numRows = accessor.numRows();
    if (Integer.MAX_VALUE - numRows < intoStart) {
      throw new ISE("too many rows!!! intoStart[%,d], numRows[%,d] combine to exceed max_int", intoStart, numRows);
    }

    for (int i = 0; i < numRows; ++i) {
      into[intoStart + i] = accessor.getObject(i);
    }
  }
}
