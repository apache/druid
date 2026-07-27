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

package org.apache.druid.segment.filter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ConciseBitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.DimExtractionFn;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.query.filter.ExtractionDimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.serde.StringUtf8ColumnIndexSupplier;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
@ParameterizedClass
@MethodSource("constructorFeeder")
public class ExtractionDimFilterTest extends InitializedNullHandlingTest
{
  private static final Map<String, String> EXTRACTION_VALUES = ImmutableMap.of(
      "foo1", "extractDimVal"
  );

  public static Stream<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{new ConciseBitmapFactory(), new ConciseBitmapSerdeFactory()},
        new Object[]{new RoaringBitmapFactory(), RoaringBitmapSerdeFactory.getInstance()}
    ).stream();
  }

  @Parameter(0)
  public BitmapFactory factory;
  @Parameter(1)
  public BitmapSerdeFactory serdeFactory;
  public ImmutableBitmap foo1BitMap;

  @BeforeEach
  public void initFields()
  {
    final MutableBitmap mutableBitmap = factory.makeEmptyMutableBitmap();
    mutableBitmap.add(1);
    foo1BitMap = factory.makeImmutableBitmap(mutableBitmap);
  }

  private final ColumnIndexSelector BITMAP_INDEX_SELECTOR = new ColumnIndexSelector()
  {
    @Nullable
    @Override
    public ColumnHolder getColumnHolder(String columnName)
    {
      return null;
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return ColumnCapabilitiesImpl.createDefault()
                                   .setType(ColumnType.STRING)
                                   .setHasMultipleValues(true)
                                   .setDictionaryEncoded(true)
                                   .setDictionaryValuesUnique(true)
                                   .setDictionaryValuesSorted(true);
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
    public ColumnIndexSupplier getIndexSupplier(String column)
    {
      if ("foo".equals(column)) {
        return new StringUtf8ColumnIndexSupplier<>(
            factory,
            GenericIndexed.fromIterable(
                Collections.singletonList(ByteBuffer.wrap(StringUtils.toUtf8("foo1"))),
                GenericIndexed.UTF8_STRATEGY
            )::singleThreaded,
            GenericIndexed.fromIterable(Collections.singletonList(foo1BitMap), serdeFactory.getObjectStrategy()),
            null
        );
      }
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
    Filter extractionFilter = new SelectorDimFilter("foo", "NFDJUKFNDSJFNS", DIM_EXTRACTION_FN).toFilter();
    ImmutableBitmap immutableBitmap = Filters.computeDefaultBitmapResults(extractionFilter, BITMAP_INDEX_SELECTOR);
    Assertions.assertEquals(0, immutableBitmap.size());
  }

  @Test
  public void testNull()
  {
    Filter extractionFilter = new SelectorDimFilter("FDHJSFFHDS", "extractDimVal", DIM_EXTRACTION_FN).toFilter();
    ImmutableBitmap immutableBitmap = Filters.computeDefaultBitmapResults(extractionFilter, BITMAP_INDEX_SELECTOR);
    Assertions.assertEquals(0, immutableBitmap.size());
  }

  @Test
  public void testNormal()
  {
    Filter extractionFilter = new SelectorDimFilter("foo", "extractDimVal", DIM_EXTRACTION_FN).toFilter();
    ImmutableBitmap immutableBitmap = Filters.computeDefaultBitmapResults(extractionFilter, BITMAP_INDEX_SELECTOR);
    Assertions.assertEquals(1, immutableBitmap.size());
  }

  @Test
  public void testOr()
  {
    Assertions.assertEquals(
        1,
        Filters.computeDefaultBitmapResults(
            Filters.toFilter(DimFilters.or(new ExtractionDimFilter("foo", "extractDimVal", DIM_EXTRACTION_FN, null))),
            BITMAP_INDEX_SELECTOR
        ).size()
    );

    Assertions.assertEquals(
        1,
        Filters.computeDefaultBitmapResults(
            Filters.toFilter(
                DimFilters.or(
                    new ExtractionDimFilter("foo", "extractDimVal", DIM_EXTRACTION_FN, null),
                    new ExtractionDimFilter("foo", "DOES NOT EXIST", DIM_EXTRACTION_FN, null)
                )
            ),
            BITMAP_INDEX_SELECTOR
        ).size()
    );
  }

  @Test
  public void testAnd()
  {
    Assertions.assertEquals(
        1,
        Filters.computeDefaultBitmapResults(
            Filters.toFilter(DimFilters.or(new ExtractionDimFilter("foo", "extractDimVal", DIM_EXTRACTION_FN, null))),
            BITMAP_INDEX_SELECTOR
        ).size()
    );

    Assertions.assertEquals(
        1,
        Filters.computeDefaultBitmapResults(
            Filters.toFilter(
                DimFilters.and(
                    new ExtractionDimFilter("foo", "extractDimVal", DIM_EXTRACTION_FN, null),
                    new ExtractionDimFilter("foo", "extractDimVal", DIM_EXTRACTION_FN, null)
                )
            ),
            BITMAP_INDEX_SELECTOR
        ).size()
    );
  }

  @Test
  public void testNot()
  {

    Assertions.assertEquals(
        1,
        Filters.computeDefaultBitmapResults(
            Filters.toFilter(DimFilters.or(new ExtractionDimFilter("foo", "extractDimVal", DIM_EXTRACTION_FN, null))),
            BITMAP_INDEX_SELECTOR
        ).size()
    );

    Assertions.assertEquals(
        1,
        Filters.computeDefaultBitmapResults(
            Filters.toFilter(DimFilters.not(new ExtractionDimFilter("foo", "DOES NOT EXIST", DIM_EXTRACTION_FN, null))),
            BITMAP_INDEX_SELECTOR
        ).size()
    );
  }
}
