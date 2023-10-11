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

import org.apache.druid.query.rowsandcols.column.accessor.LongColumnAccessorBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ColumnAccessorsTest
{
  private TestAccessorShim mode;
  private TestAccessorShim accessor;
  private Object expectedValue;

  @Parameters
  public static List<Object[]> getParameters()
  {
    List<Object[]> ret = new ArrayList<>();

    ret.add(new Object[] {TestAccessorShim.LONG, 42L});

    return ret;
  }

  enum TestAccessorShim  {
    LONG    {
      @Override
      ColumnAccessor getColumnAccessor(Object value)
      {
        Long val = (Long) value;
        return new LongColumnAccessorBase()
        {
          @Override
          public int numRows()
          {
            return 1;
          }

          @Override
          public boolean isNull(int rowNum)
          {
            return val == null;
          }

          @Override
          public long getLong(int rowNum)
          {
            return val;
          }
        };
      }
    };

    abstract ColumnAccessor getColumnAccessor(Object val);
  }

  public ColumnAccessorsTest(TestAccessorShim accessor, Object expectedValue)
  {
    this.mode = accessor;
    this.expectedValue = expectedValue;
  }

  @Test
  public void testValue()
  {
    ColumnAccessor acc = mode.getColumnAccessor(expectedValue);

    assertFalse(acc.isNull(0));
    assertEquals(expectedValue, acc.getLong(0));
    assertEquals(expectedValue, acc.getObject(0));
  }

  @Test
  public void testSanity2()
  {
    ColumnAccessor acc = mode.getColumnAccessor(null);

    assertTrue(acc.isNull(0));
    assertEquals(null, acc.getObject(0));
  }

}
