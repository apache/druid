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

package org.apache.druid.segment.virtual;

import org.apache.druid.error.DruidException;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ObjectBasedColumnSelector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestObjectBasedColumnSelector
{
  static class MyObjectColumnSelector extends ObjectBasedColumnSelector<Object>
  {
    private Object obj;

    MyObjectColumnSelector(Object obj)
    {
      this.obj = obj;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
    }

    @Override
    public Object getObject()
    {
      return obj;
    }

    @Override
    public Class<? extends Object> classOfObject()
    {
      if (obj == null) {
        return null;
      }
      return obj.getClass();
    }
  }

  @Test
  public void testNull()
  {
    MyObjectColumnSelector selector = new MyObjectColumnSelector(null);
    assertThrows(DruidException.class, () -> selector.getFloat());
    assertThrows(DruidException.class, () -> selector.getDouble());
    assertThrows(DruidException.class, () -> selector.getLong());
    assertTrue(selector.isNull());
  }

  @Test
  public void testLong()
  {
    MyObjectColumnSelector selector = new MyObjectColumnSelector(Long.valueOf(11L));
    assertFalse(selector.isNull());
    assertEquals(11f, selector.getLong(), 0.0f);
  }

  @Test
  public void testInvalidIsNullUse()
  {
    MyObjectColumnSelector selector = new MyObjectColumnSelector(getClass());

    DruidException e = assertThrows(DruidException.class, () -> selector.isNull());

    assertEquals(
        e.getMessage(),
        "isNull() may only be called in case the underlying object is a Number but it was [java.lang.Class]"
    );
  }
}
