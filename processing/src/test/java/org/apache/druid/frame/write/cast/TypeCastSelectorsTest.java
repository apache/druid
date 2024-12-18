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

package org.apache.druid.frame.write.cast;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Map;

public class TypeCastSelectorsTest extends InitializedNullHandlingTest
{
  private final ColumnSelectorFactory testColumnSelectorFactory = new TestColumnSelectorFactory(
      RowSignature.builder()
                  .add("x", ColumnType.STRING)
                  .add("y", ColumnType.STRING)
                  .add("z", ColumnType.STRING)
                  .add("da", ColumnType.DOUBLE_ARRAY)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("x", "12.3")
                  .put("y", "abc")
                  .put("da", new Object[]{1.2d, 2.3d})
                  .build() // z is null
  );

  @Test
  public void test_readXAsLong()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "x", ColumnType.LONG);

    Assert.assertEquals(12L, selector.getLong());
    Assert.assertEquals(12d, selector.getDouble(), 0);
    Assert.assertEquals(12f, selector.getFloat(), 0);
    Assert.assertFalse(selector.isNull());
    Assert.assertEquals(12L, selector.getObject());
  }

  @Test
  public void test_readXAsDouble()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "x", ColumnType.DOUBLE);

    Assert.assertEquals(12L, selector.getLong());
    Assert.assertEquals(12.3d, selector.getDouble(), 0);
    Assert.assertEquals(12.3f, selector.getFloat(), 0);
    Assert.assertFalse(selector.isNull());
    Assert.assertEquals(12.3d, selector.getObject());
  }

  @Test
  public void test_readXAsFloat()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "x", ColumnType.FLOAT);

    Assert.assertEquals(12L, selector.getLong());
    Assert.assertEquals(12.3d, selector.getDouble(), 0.001);
    Assert.assertEquals(12.3f, selector.getFloat(), 0);
    Assert.assertFalse(selector.isNull());
    Assert.assertEquals(12.3d, selector.getObject());
  }

  @Test
  public void test_readXAsLongArray()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "x", ColumnType.LONG_ARRAY);

    Assert.assertThrows(DruidException.class, selector::getLong);
    Assert.assertThrows(DruidException.class, selector::getDouble);
    Assert.assertThrows(DruidException.class, selector::getFloat);
    Assert.assertThrows(DruidException.class, selector::isNull);
    Assert.assertArrayEquals(new Object[]{12L}, (Object[]) selector.getObject());
  }

  @Test
  public void test_readXAsStringArray()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "x", ColumnType.STRING_ARRAY);

    Assert.assertThrows(DruidException.class, selector::getLong);
    Assert.assertThrows(DruidException.class, selector::getDouble);
    Assert.assertThrows(DruidException.class, selector::getFloat);
    Assert.assertThrows(DruidException.class, selector::isNull);
    Assert.assertArrayEquals(new Object[]{"12.3"}, (Object[]) selector.getObject());
  }

  @Test
  public void test_readYAsLong()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "y", ColumnType.LONG);

    Assert.assertEquals(0L, selector.getLong());
    Assert.assertEquals(0d, selector.getDouble(), 0);
    Assert.assertEquals(0f, selector.getFloat(), 0);
    Assert.assertTrue(selector.isNull());
    Assert.assertNull(selector.getObject());
  }

  @Test
  public void test_readYAsDouble()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "y", ColumnType.DOUBLE);

    Assert.assertEquals(0L, selector.getLong());
    Assert.assertEquals(0d, selector.getDouble(), 0);
    Assert.assertEquals(0f, selector.getFloat(), 0);
    Assert.assertTrue(selector.isNull());
    Assert.assertNull(selector.getObject());
  }

  @Test
  public void test_readYAsFloat()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "y", ColumnType.FLOAT);

    Assert.assertEquals(0L, selector.getLong());
    Assert.assertEquals(0d, selector.getDouble(), 0);
    Assert.assertEquals(0f, selector.getFloat(), 0);
    Assert.assertTrue(selector.isNull());
    Assert.assertNull(selector.getObject());
  }

  @Test
  public void test_readYAsLongArray()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "y", ColumnType.LONG_ARRAY);

    Assert.assertThrows(DruidException.class, selector::getLong);
    Assert.assertThrows(DruidException.class, selector::getDouble);
    Assert.assertThrows(DruidException.class, selector::getFloat);
    Assert.assertThrows(DruidException.class, selector::isNull);
    Assert.assertArrayEquals(new Object[]{null}, (Object[]) selector.getObject());
  }

  @Test
  public void test_readYAsStringArray()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "y", ColumnType.STRING_ARRAY);

    Assert.assertThrows(DruidException.class, selector::getLong);
    Assert.assertThrows(DruidException.class, selector::getDouble);
    Assert.assertThrows(DruidException.class, selector::getFloat);
    Assert.assertThrows(DruidException.class, selector::isNull);
    Assert.assertArrayEquals(new Object[]{"abc"}, (Object[]) selector.getObject());
  }

  @Test
  public void test_readZAsLong()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "z", ColumnType.LONG);

    Assert.assertEquals(0L, selector.getLong());
    Assert.assertEquals(0d, selector.getDouble(), 0);
    Assert.assertEquals(0f, selector.getFloat(), 0);
    Assert.assertTrue(selector.isNull());
    Assert.assertNull(selector.getObject());
  }

  @Test
  public void test_readZAsDouble()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "z", ColumnType.DOUBLE);

    Assert.assertEquals(0L, selector.getLong());
    Assert.assertEquals(0d, selector.getDouble(), 0);
    Assert.assertEquals(0f, selector.getFloat(), 0);
    Assert.assertTrue(selector.isNull());
    Assert.assertNull(selector.getObject());
  }

  @Test
  public void test_readZAsFloat()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "z", ColumnType.FLOAT);

    Assert.assertEquals(0L, selector.getLong());
    Assert.assertEquals(0d, selector.getDouble(), 0);
    Assert.assertEquals(0f, selector.getFloat(), 0);
    Assert.assertTrue(selector.isNull());
    Assert.assertNull(selector.getObject());
  }

  @Test
  public void test_readZAsLongArray()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "z", ColumnType.LONG_ARRAY);

    Assert.assertThrows(DruidException.class, selector::getLong);
    Assert.assertThrows(DruidException.class, selector::getDouble);
    Assert.assertThrows(DruidException.class, selector::getFloat);
    Assert.assertThrows(DruidException.class, selector::isNull);
    Assert.assertNull(selector.getObject());
  }

  @Test
  public void test_readZAsStringArray()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "z", ColumnType.STRING_ARRAY);

    Assert.assertThrows(DruidException.class, selector::getLong);
    Assert.assertThrows(DruidException.class, selector::getDouble);
    Assert.assertThrows(DruidException.class, selector::getFloat);
    Assert.assertThrows(DruidException.class, selector::isNull);
    Assert.assertNull(selector.getObject());
  }

  @Test
  public void test_readDaAsLong()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "da", ColumnType.LONG);

    Assert.assertEquals(0L, selector.getLong());
    Assert.assertEquals(0d, selector.getDouble(), 0);
    Assert.assertEquals(0f, selector.getFloat(), 0);
    Assert.assertTrue(selector.isNull());
    Assert.assertNull(selector.getObject());
  }

  @Test
  public void test_readDaAsDouble()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "da", ColumnType.DOUBLE);

    Assert.assertEquals(0L, selector.getLong());
    Assert.assertEquals(0d, selector.getDouble(), 0);
    Assert.assertEquals(0f, selector.getFloat(), 0);
    Assert.assertTrue(selector.isNull());
    Assert.assertNull(selector.getObject());
  }

  @Test
  public void test_readDaAsFloat()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "da", ColumnType.FLOAT);

    Assert.assertEquals(0L, selector.getLong());
    Assert.assertEquals(0d, selector.getDouble(), 0);
    Assert.assertEquals(0f, selector.getFloat(), 0);
    Assert.assertTrue(selector.isNull());
    Assert.assertNull(selector.getObject());
  }

  @Test
  public void test_readDaAsLongArray()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "da", ColumnType.LONG_ARRAY);

    Assert.assertThrows(DruidException.class, selector::getLong);
    Assert.assertThrows(DruidException.class, selector::getDouble);
    Assert.assertThrows(DruidException.class, selector::getFloat);
    Assert.assertThrows(DruidException.class, selector::isNull);
    Assert.assertArrayEquals(new Object[]{1L, 2L}, (Object[]) selector.getObject());
  }

  @Test
  public void test_readDaAsStringArray()
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(testColumnSelectorFactory, "da", ColumnType.STRING_ARRAY);

    Assert.assertThrows(DruidException.class, selector::getLong);
    Assert.assertThrows(DruidException.class, selector::getDouble);
    Assert.assertThrows(DruidException.class, selector::getFloat);
    Assert.assertThrows(DruidException.class, selector::isNull);
    Assert.assertArrayEquals(new Object[]{"1.2", "2.3"}, (Object[]) selector.getObject());
  }

  /**
   * Implementation that returns a fixed value per column from {@link ColumnValueSelector#getObject()}. Other
   * methods, such as {@link ColumnValueSelector#getLong()} throw exceptions. This is meant to help validate
   * that those other methods are *not* called.
   */
  private static class TestColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final RowSignature signature;
    private final Map<String, Object> columnValues;

    public TestColumnSelectorFactory(final RowSignature signature, final Map<String, Object> columnValues)
    {
      this.signature = signature;
      this.columnValues = columnValues;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
    {
      return new ColumnValueSelector<>()
      {
        @Override
        public double getDouble()
        {
          throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public float getFloat()
        {
          throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public long getLong()
        {
          throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public boolean isNull()
        {
          throw new UnsupportedOperationException("Should not be called");
        }

        @Nullable
        @Override
        public Object getObject()
        {
          return columnValues.get(columnName);
        }

        @Override
        public Class<?> classOfObject()
        {
          throw new UnsupportedOperationException("Should not be called");
        }
      };
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return signature.getColumnCapabilities(column);
    }
  }
}
