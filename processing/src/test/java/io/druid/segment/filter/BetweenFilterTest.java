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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ConciseBitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.bitmap.RoaringBitmapFactory;
import com.metamx.collections.spatial.ImmutableRTree;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.data.ArrayIndexed;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.ListIndexed;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by zhxiaog on 15/11/16.
 */
@RunWith(Parameterized.class)
public class BetweenFilterTest
{

  private static final Map<String, List<String>> DIM_VALS = ImmutableMap.<String, List<String>>of(
      "foo", Arrays.asList("0.1", "0.5", "9.0", "10.0"),
      "bar", Arrays.asList("abc", "abb", "abc", "abd")
  );

  private final BitmapFactory factory;

  public BetweenFilterTest(BitmapFactory factory)
  {
    this.factory = factory;
  }

  private final BitmapIndexSelector BITMAP_INDEX_SELECTOR = new BitmapIndexSelector()
  {
    @Override
    public Indexed<String> getDimensionValues(String dimension)
    {
      final List<String> vals = DIM_VALS.get(dimension);
      return vals == null ? null : new ListIndexed<>(vals, String.class);
    }

    @Override
    public int getNumRows()
    {
      return 3;
    }

    @Override
    public BitmapFactory getBitmapFactory()
    {
      return factory;
    }

    @Override
    public ImmutableBitmap getBitmapIndex(String dimension, String value)
    {
      List<String> dimValues = DIM_VALS.get(dimension);
      if (dimValues != null && dimValues.contains(value)) {
        MutableBitmap bitMap = factory.makeEmptyMutableBitmap();
        bitMap.add(dimValues.indexOf(value) + 1);
        return factory.makeImmutableBitmap(bitMap);
      } else {
        return factory.makeEmptyImmutableBitmap();
      }
    }

    @Override
    public ImmutableRTree getSpatialIndex(String dimension)
    {
      return null;
    }
  };

  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{new ConciseBitmapFactory()},
        new Object[]{new RoaringBitmapFactory()}
    );
  }

  @Test
  public void testGetBitmapIndex_Number() throws Exception
  {
    BetweenFilter filter = new BetweenFilter("foo", true, 0.5f, 9.0f);
    ImmutableBitmap bitMap = filter.getBitmapIndex(BITMAP_INDEX_SELECTOR);
    Assert.assertEquals(2, bitMap.size());
  }

  @Test
  public void testGetBitmapIndex_String() throws Exception
  {
    BetweenFilter filter = new BetweenFilter("bar", false, "abb", "abc");
    ImmutableBitmap bitMap = filter.getBitmapIndex(BITMAP_INDEX_SELECTOR);
    Assert.assertEquals(2, bitMap.size());
  }
}
