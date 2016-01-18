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
import io.druid.query.extraction.DimExtractionFn;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.ExtractionDimFilter;
import io.druid.segment.data.ArrayIndexed;
import io.druid.segment.data.Indexed;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Map;

/**
 *
 */
@RunWith(Parameterized.class)
public class ExtractionDimFilterTest
{
  private static final Map<String, String[]> DIM_VALS = ImmutableMap.<String, String[]>of(
      "foo", new String[]{"foo1", "foo2", "foo3"},
      "bar", new String[]{"bar1"},
      "baz", new String[]{"foo1"}
  );

  private static final Map<String, String> EXTRACTION_VALUES = ImmutableMap.of(
      "foo1", "extractDimVal"
  );

  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{new ConciseBitmapFactory()},
        new Object[]{new RoaringBitmapFactory()}
    );
  }

  public ExtractionDimFilterTest(BitmapFactory bitmapFactory)
  {
    final MutableBitmap mutableBitmap = bitmapFactory.makeEmptyMutableBitmap();
    mutableBitmap.add(1);
    this.foo1BitMap = bitmapFactory.makeImmutableBitmap(mutableBitmap);
    this.factory = bitmapFactory;
  }

  private final BitmapFactory factory;
  private final ImmutableBitmap foo1BitMap;

  private final BitmapIndexSelector BITMAP_INDEX_SELECTOR = new BitmapIndexSelector()
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
      return factory;
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

    @Override
    public ExtractionType getExtractionType()
    {
      return ExtractionType.MANY_TO_ONE;
    }
  };

  @Test
  public void testEmpty()
  {
    ExtractionFilter extractionFilter = new ExtractionFilter(
        "foo", "NFDJUKFNDSJFNS", DIM_EXTRACTION_FN
    );
    ImmutableBitmap immutableBitmap = extractionFilter.getBitmapIndex(BITMAP_INDEX_SELECTOR);
    Assert.assertEquals(0, immutableBitmap.size());
  }

  @Test
  public void testNull()
  {
    ExtractionFilter extractionFilter = new ExtractionFilter(
        "FDHJSFFHDS", "extractDimVal", DIM_EXTRACTION_FN
    );
    ImmutableBitmap immutableBitmap = extractionFilter.getBitmapIndex(BITMAP_INDEX_SELECTOR);
    Assert.assertEquals(0, immutableBitmap.size());
  }

  @Test
  public void testNormal()
  {
    ExtractionFilter extractionFilter = new ExtractionFilter(
        "foo", "extractDimVal", DIM_EXTRACTION_FN
    );
    ImmutableBitmap immutableBitmap = extractionFilter.getBitmapIndex(BITMAP_INDEX_SELECTOR);
    Assert.assertEquals(1, immutableBitmap.size());
  }

  @Test
  public void testOr()
  {
    Assert.assertEquals(
        1, Filters.convertDimensionFilters(
            DimFilters.or(
                new ExtractionDimFilter(
                    "foo",
                    "extractDimVal",
                    DIM_EXTRACTION_FN,
                    null
                )
            )
        ).getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );

    Assert.assertEquals(
        1,
        Filters.convertDimensionFilters(
            DimFilters.or(
                new ExtractionDimFilter(
                    "foo",
                    "extractDimVal",
                    DIM_EXTRACTION_FN,
                    null
                ),
                new ExtractionDimFilter(
                    "foo",
                    "DOES NOT EXIST",
                    DIM_EXTRACTION_FN,
                    null
                )
            )
        ).getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );
  }

  @Test
  public void testAnd()
  {
    Assert.assertEquals(
        1, Filters.convertDimensionFilters(
            DimFilters.or(
                new ExtractionDimFilter(
                    "foo",
                    "extractDimVal",
                    DIM_EXTRACTION_FN,
                    null
                )
            )
        ).getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );

    Assert.assertEquals(
        1,
        Filters.convertDimensionFilters(
            DimFilters.and(
                new ExtractionDimFilter(
                    "foo",
                    "extractDimVal",
                    DIM_EXTRACTION_FN,
                    null
                ),
                new ExtractionDimFilter(
                    "foo",
                    "extractDimVal",
                    DIM_EXTRACTION_FN,
                    null
                )
            )
        ).getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );
  }

  @Test
  public void testNot()
  {

    Assert.assertEquals(
        1, Filters.convertDimensionFilters(
            DimFilters.or(
                new ExtractionDimFilter(
                    "foo",
                    "extractDimVal",
                    DIM_EXTRACTION_FN,
                    null
                )
            )
        ).getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );

    Assert.assertEquals(
        1,
        Filters.convertDimensionFilters(
            DimFilters.not(
                new ExtractionDimFilter(
                    "foo",
                    "DOES NOT EXIST",
                    DIM_EXTRACTION_FN,
                    null
                )
            )
        ).getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );
  }
}
