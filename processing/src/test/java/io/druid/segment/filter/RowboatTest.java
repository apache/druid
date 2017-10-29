/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.filter;

import io.druid.segment.DimensionHandler;
import io.druid.segment.Rowboat;
import io.druid.segment.StringDimensionHandler;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class RowboatTest
{
  private static DimensionHandler[] getDefaultHandlers(int size)
  {
    DimensionHandler[] handlers = new DimensionHandler[size];
    for (int i = 0; i < size; i++) {
      handlers[i] = new StringDimensionHandler(String.valueOf(i), null);
    }
    return handlers;
  }

  @Test
  public void testRowboatCompare()
  {
    DimensionHandler[] handlers = getDefaultHandlers(3);
    Rowboat rb1 = new Rowboat(12345L, new int[][]{new int[]{1}, new int[]{2}}, new Object[]{new Integer(7)}, 5, handlers);
    Rowboat rb2 = new Rowboat(12345L, new int[][]{new int[]{1}, new int[]{2}}, new Object[]{new Integer(7)}, 5, handlers);
    Assert.assertEquals(0, rb1.compareTo(rb2));

    Rowboat rb3 = new Rowboat(12345L, new int[][]{new int[]{3}, new int[]{2}}, new Object[]{new Integer(7)}, 5, handlers);
    Assert.assertNotEquals(0, rb1.compareTo(rb3));
  }

  @Test
  public void testBiggerCompare()
  {
    DimensionHandler[] handlers = getDefaultHandlers(14);

    Rowboat rb1 = new Rowboat(
        0,
        new int[][]{
            new int[]{0},
            new int[]{138},
            new int[]{44},
            new int[]{374},
            new int[]{0},
            new int[]{0},
            new int[]{552},
            new int[]{338},
            new int[]{910},
            new int[]{25570},
            new int[]{9},
            new int[]{0},
            new int[]{0},
            new int[]{0}
        },
        new Object[]{1.0, 47.0, "someMetric"},
        0,
        handlers
    );

    Rowboat rb2 = new Rowboat(
        0,
        new int[][]{
            new int[]{0},
            new int[]{138},
            new int[]{44},
            new int[]{374},
            new int[]{0},
            new int[]{0},
            new int[]{553},
            new int[]{338},
            new int[]{910},
            new int[]{25580},
            new int[]{9},
            new int[]{0},
            new int[]{0},
            new int[]{0}
        },
        new Object[]{1.0, 47.0, "someMetric"},
        0,
        handlers
    );

    Assert.assertNotEquals(0, rb1.compareTo(rb2));
  }

  @Test
  public void testToString()
  {
    DimensionHandler[] handlers = getDefaultHandlers(2);

    Assert.assertEquals(
        "Rowboat{timestamp=1970-01-01T00:00:00.000Z, dims=[[1], [2]], metrics=[someMetric], comprisedRows={}}",
        new Rowboat(0, new int[][]{new int[]{1}, new int[]{2}}, new Object[]{"someMetric"}, 5, handlers).toString()
    );
  }

  @Test
  public void testLotsONullString()
  {
    DimensionHandler[] handlers = getDefaultHandlers(0);

    Assert.assertEquals(
        "Rowboat{timestamp=1970-01-01T00:00:00.000Z, dims=null, metrics=null, comprisedRows={}}",
        new Rowboat(0, null, null, 5, handlers).toString()
    );
  }
}
