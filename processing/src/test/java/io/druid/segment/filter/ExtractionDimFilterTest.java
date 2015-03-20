/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.filter;

import com.google.common.collect.ImmutableMap;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ConciseBitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.WrappedConciseBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import io.druid.query.extraction.DimExtractionFn;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.segment.data.ArrayIndexed;
import io.druid.segment.data.Indexed;
import it.uniroma3.mat.extendedset.intset.ConciseSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

/**
 *
 */
public class ExtractionDimFilterTest
{
  private static final Map<String, String[]> DIM_VALS = ImmutableMap.<String, String[]>of(
      "foo", new String[]{"foo1","foo2","foo3"},
      "bar", new String[]{"bar1"},
      "baz", new String[]{"foo1"}
  );

  private static final Map<String, String> EXTRACTION_VALUES = ImmutableMap.of(
      "foo1","extractDimVal"
  );

  private static ImmutableBitmap foo1BitMap;
  @BeforeClass
  public static void setupStatic(){
    final ConciseSet conciseSet = new ConciseSet();
    conciseSet.add(1);
    foo1BitMap = new WrappedConciseBitmap(conciseSet);
  }
  private static final BitmapIndexSelector BITMAP_INDEX_SELECTOR = new BitmapIndexSelector()
  {
    @Override
    public Indexed<String> getDimensionValues(String dimension)
    {
      final String[] vals = DIM_VALS.get(dimension);
      return vals == null ? null : new ArrayIndexed<String>(vals, String.class);
    }

    @Override
    public int getNumRows()
    {
      return 1;
    }

    @Override
    public BitmapFactory getBitmapFactory()
    {
      return new ConciseBitmapFactory();
    }

    @Override
    public ImmutableBitmap getBitmapIndex(String dimension, String value)
    {
      return "foo1".equals(value) ? foo1BitMap : null;
    }

    @Override
    public ImmutableRTree getSpatialIndex(String dimension)
    {
      return null;
    }
  };
  private static final ExtractionFn DIM_EXTRACTION_FN = new DimExtractionFn()
  {
    @Override
    public byte[] getCacheKey()
    {
      return new byte[0];
    }

    @Override
    public String apply(String dimValue)
    {
      final String retval = EXTRACTION_VALUES.get(dimValue);
      return retval == null ? dimValue : retval;
    }

    @Override
    public boolean preservesOrdering()
    {
      return false;
    }
  };

  @Test
  public void testEmpty(){
    ExtractionFilter extractionFilter = new ExtractionFilter(
        "foo", "NFDJUKFNDSJFNS", DIM_EXTRACTION_FN
    );
    ImmutableBitmap immutableBitmap = extractionFilter.getBitmapIndex(BITMAP_INDEX_SELECTOR);
    Assert.assertEquals(0, immutableBitmap.size());
  }

  @Test
  public void testNull(){
    ExtractionFilter extractionFilter = new ExtractionFilter(
        "FDHJSFFHDS", "extractDimVal", DIM_EXTRACTION_FN
    );
    ImmutableBitmap immutableBitmap = extractionFilter.getBitmapIndex(BITMAP_INDEX_SELECTOR);
    Assert.assertEquals(0, immutableBitmap.size());
  }

  @Test
  public void testNormal(){
    ExtractionFilter extractionFilter = new ExtractionFilter(
        "foo", "extractDimVal", DIM_EXTRACTION_FN
    );
    ImmutableBitmap immutableBitmap = extractionFilter.getBitmapIndex(BITMAP_INDEX_SELECTOR);
    Assert.assertEquals(1, immutableBitmap.size());
  }
}
