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

import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Test;

public class NullColumnTest
{

  @Test
  public void testSanity()
  {
    NullColumn col = new NullColumn(ColumnType.UNKNOWN_COMPLEX, 10);
    ColumnAccessor accessor = col.toAccessor();
    Assert.assertEquals(10, accessor.numRows());

    for (int i = 0; i < 10; ++i) {
      Assert.assertTrue(accessor.isNull(i));
      Assert.assertNull(accessor.getObject(i));
      Assert.assertEquals(0, accessor.getInt(i));
      Assert.assertEquals(0, accessor.getLong(i));
      Assert.assertEquals(0.0, accessor.getFloat(i), 0);
      Assert.assertEquals(0.0, accessor.getDouble(i), 0);
      for (int j = 0; j < i; ++j) {
        Assert.assertEquals(0, accessor.compareRows(j, i));
      }
    }
  }
}
