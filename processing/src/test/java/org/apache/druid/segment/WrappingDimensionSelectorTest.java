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

package org.apache.druid.segment;

import org.apache.druid.common.config.NullHandling;
import org.junit.Assert;
import org.junit.Test;

public class WrappingDimensionSelectorTest
{
  @Test
  public void testLongWrappingDimensionSelector()
  {
    Long[] vals = new Long[]{24L, null, 50L, 0L, -60L};
    TestNullableLongColumnSelector lngSelector = new TestNullableLongColumnSelector(vals);

    LongWrappingDimensionSelector lngWrapSelector = new LongWrappingDimensionSelector(lngSelector, null);
    Assert.assertEquals(24L, lngSelector.getLong());
    Assert.assertEquals("24", lngWrapSelector.getValue());

    lngSelector.increment();
    if (NullHandling.sqlCompatible()) {
      Assert.assertTrue(lngSelector.isNull());
    } else {
      Assert.assertEquals(0L, lngSelector.getLong());
    }

    lngSelector.increment();
    Assert.assertEquals(50L, lngSelector.getLong());
    Assert.assertEquals("50", lngWrapSelector.getValue());

    lngSelector.increment();
    Assert.assertEquals(0L, lngSelector.getLong());
    Assert.assertEquals("0", lngWrapSelector.getValue());

    lngSelector.increment();
    Assert.assertEquals(-60L, lngSelector.getLong());
    Assert.assertEquals("-60", lngWrapSelector.getValue());
  }

  @Test
  public void testDoubleWrappingDimensionSelector()
  {
    Double[] vals = new Double[]{32.0d, null, 5.0d, 0.0d, -45.0d};
    TestNullableDoubleColumnSelector dblSelector = new TestNullableDoubleColumnSelector(vals);

    DoubleWrappingDimensionSelector dblWrapSelector = new DoubleWrappingDimensionSelector(dblSelector, null);
    Assert.assertEquals(32.0d, dblSelector.getDouble(), 0);
    Assert.assertEquals("32.0", dblWrapSelector.getValue());

    dblSelector.increment();
    if (NullHandling.sqlCompatible()) {
      Assert.assertTrue(dblSelector.isNull());
    } else {
      Assert.assertEquals(0d, dblSelector.getDouble(), 0);
    }

    dblSelector.increment();
    Assert.assertEquals(5.0d, dblSelector.getDouble(), 0);
    Assert.assertEquals("5.0", dblWrapSelector.getValue());

    dblSelector.increment();
    Assert.assertEquals(0.0d, dblSelector.getDouble(), 0);
    Assert.assertEquals("0.0", dblWrapSelector.getValue());

    dblSelector.increment();
    Assert.assertEquals(-45.0d, dblSelector.getDouble(), 0);
    Assert.assertEquals("-45.0", dblWrapSelector.getValue());
  }

  @Test
  public void testFloatWrappingDimensionSelector()
  {
    Float[] vals = new Float[]{32.0f, null, 5.0f, 0.0f, -45.0f};
    TestNullableFloatColumnSelector flSelector = new TestNullableFloatColumnSelector(vals);

    FloatWrappingDimensionSelector flWrapSelector = new FloatWrappingDimensionSelector(flSelector, null);
    Assert.assertEquals(32.0f, flSelector.getFloat(), 0);
    Assert.assertEquals("32.0", flWrapSelector.getValue());

    flSelector.increment();
    if (NullHandling.sqlCompatible()) {
      Assert.assertTrue(flSelector.isNull());
    } else {
      Assert.assertEquals(0f, flSelector.getFloat(), 0);
    }

    flSelector.increment();
    Assert.assertEquals(5.0f, flSelector.getFloat(), 0);
    Assert.assertEquals("5.0", flWrapSelector.getValue());

    flSelector.increment();
    Assert.assertEquals(0.0f, flSelector.getFloat(), 0);
    Assert.assertEquals("0.0", flWrapSelector.getValue());

    flSelector.increment();
    Assert.assertEquals(-45.0f, flSelector.getFloat(), 0);
    Assert.assertEquals("-45.0", flWrapSelector.getValue());
  }
}
