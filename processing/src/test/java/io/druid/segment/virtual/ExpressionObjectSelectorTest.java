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

package io.druid.segment.virtual;

import com.google.common.base.Supplier;
import io.druid.common.guava.SettableSupplier;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ExpressionObjectSelectorTest
{
  @Test
  public void testSupplierFromLongSelector()
  {
    final Supplier<Number> supplier = ExpressionObjectSelector.supplierFromLongSelector(
        new LongColumnSelector()
        {
          @Override
          public long get()
          {
            return 1L;
          }
        }
    );

    Assert.assertEquals(1L, supplier.get());
  }

  @Test
  public void testSupplierFromFloatSelector()
  {
    final Supplier<Number> supplier = ExpressionObjectSelector.supplierFromFloatSelector(
        new FloatColumnSelector()
        {
          @Override
          public float get()
          {
            return 0.1f;
          }
        }
    );

    Assert.assertEquals(0.1f, supplier.get());
  }

  @Test
  public void testSupplierFromObjectSelectorObject()
  {
    final SettableSupplier<Object> settableSupplier = new SettableSupplier<>();
    final Supplier<Number> supplier = ExpressionObjectSelector.supplierFromObjectSelector(
        selectorFromSupplier(settableSupplier, Object.class)
    );

    Assert.assertNotNull(supplier);
    Assert.assertEquals(null, supplier.get());

    settableSupplier.set(1.1f);
    Assert.assertEquals(1.1f, supplier.get());

    settableSupplier.set(1L);
    Assert.assertEquals(1L, supplier.get());

    settableSupplier.set("1234");
    Assert.assertEquals(1234L, supplier.get());

    settableSupplier.set("1.234");
    Assert.assertEquals(1.234d, supplier.get());
  }

  @Test
  public void testSupplierFromObjectSelectorNumber()
  {
    final SettableSupplier<Number> settableSupplier = new SettableSupplier<>();
    final Supplier<Number> supplier = ExpressionObjectSelector.supplierFromObjectSelector(
        selectorFromSupplier(settableSupplier, Number.class)
    );


    Assert.assertNotNull(supplier);
    Assert.assertEquals(null, supplier.get());

    settableSupplier.set(1.1f);
    Assert.assertEquals(1.1f, supplier.get());

    settableSupplier.set(1L);
    Assert.assertEquals(1L, supplier.get());
  }

  @Test
  public void testSupplierFromObjectSelectorString()
  {
    final SettableSupplier<String> settableSupplier = new SettableSupplier<>();
    final Supplier<Number> supplier = ExpressionObjectSelector.supplierFromObjectSelector(
        selectorFromSupplier(settableSupplier, String.class)
    );

    Assert.assertNotNull(supplier);
    Assert.assertEquals(null, supplier.get());

    settableSupplier.set("1.1");
    Assert.assertEquals(1.1d, supplier.get());

    settableSupplier.set("1");
    Assert.assertEquals(1L, supplier.get());
  }

  @Test
  public void testSupplierFromObjectSelectorList()
  {
    final SettableSupplier<List> settableSupplier = new SettableSupplier<>();
    final Supplier<Number> supplier = ExpressionObjectSelector.supplierFromObjectSelector(
        selectorFromSupplier(settableSupplier, List.class)
    );

    // List can't be a number, so supplierFromObjectSelector should return null.
    Assert.assertNull(supplier);
  }

  private static <T> ObjectColumnSelector<T> selectorFromSupplier(final Supplier<T> supplier, final Class<T> clazz)
  {
    return new ObjectColumnSelector<T>()
    {
      @Override
      public Class<T> classOfObject()
      {
        return clazz;
      }

      @Override
      public T get()
      {
        return supplier.get();
      }
    };
  }
}
