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

import org.apache.druid.query.rowsandcols.column.accessor.DoubleColumnAccessorBase;
import org.apache.druid.query.rowsandcols.column.accessor.FloatColumnAccessorBase;
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
  private TestAccessorShim testAccessor;

  @Parameters
  public static List<Object[]> getParameters()
  {
    List<Object[]> ret = new ArrayList<>();

    for (TestAccessorShim accessor : TestAccessorShim.values()) {
      ret.add(new Object[] {accessor});
    }

    return ret;
  }

  enum TestAccessorShim
  {
    LONG {
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

      @Override
      protected Object getSomeValue()
      {
        return 42L;
      }


    },
    FLOAT {
      @Override
      ColumnAccessor getColumnAccessor(Object value)
      {
        Float val = (Float) value;
        return new FloatColumnAccessorBase()
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
          public float getFloat(int rowNum)
          {
            return val;
          }
        };
      }

      @Override
      protected Object getSomeValue()
      {
        return 42.1F;
      }
    },
    DOUBLE {
      @Override
      ColumnAccessor getColumnAccessor(Object value)
      {
        Double val = (Double) value;
        return new DoubleColumnAccessorBase()
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
          public double getDouble(int rowNum)
          {
            return val;
          }
        };
      }

      @Override
      protected Object getSomeValue()
      {
        return 42.1D;
      }
    };

    abstract ColumnAccessor getColumnAccessor(Object val);

    protected abstract Object getSomeValue();
  }

  public ColumnAccessorsTest(TestAccessorShim accessor)
  {
    this.testAccessor = accessor;
  }

  @Test
  public void testSomeValue()
  {
    Object expectedValue = testAccessor.getSomeValue();
    ColumnAccessor acc = testAccessor.getColumnAccessor(expectedValue);

    assertFalse(acc.isNull(0));
    assertEquals(expectedValue, acc.getObject(0));
  }

  @Test
  public void testNull()
  {
    ColumnAccessor acc = testAccessor.getColumnAccessor(null);

    assertTrue(acc.isNull(0));
    assertEquals(null, acc.getObject(0));
  }
}
