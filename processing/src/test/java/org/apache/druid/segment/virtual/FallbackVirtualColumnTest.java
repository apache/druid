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

import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.ColumnSelectorColumnIndexSelector;
import org.apache.druid.segment.ConstantDimensionSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.TestColumnSelector;
import org.apache.druid.segment.TestColumnSelectorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.TestVectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;

@SuppressWarnings("ALL")
public class FallbackVirtualColumnTest
{
  static {
    NullHandling.initializeForTests();
  }

  @Test
  public void testGetOutputName()
  {
    Assert.assertEquals("slimshady", makeCol("slimshady", "test1", "test2").getOutputName());
  }

  @Test
  public void testGetColumns()
  {
    Assert.assertEquals(
        Arrays.asList(DefaultDimensionSpec.of("test1"), DefaultDimensionSpec.of("test2")),
        makeCol("slimshady", "test1", "test2").getColumns()
    );
  }

  @Test
  public void testGetCacheKey()
  {
    Assert.assertArrayEquals(
        new CacheKeyBuilder((byte) 0x3)
            .appendString("slimshady")
            .appendCacheable(DefaultDimensionSpec.of("test1"))
            .appendCacheable(DefaultDimensionSpec.of("test2"))
            .appendCacheable(DefaultDimensionSpec.of("oneMore"))
            .build(),
        makeCol("slimshady", "test1", "test2", "oneMore").getCacheKey()
    );
  }

  @Test
  public void testMakeDimensionSelector()
  {
    final FallbackVirtualColumn col = makeCol("slimshady", "colA", "colB", "colC");

    final ConstantDimensionSelector colA = new ConstantDimensionSelector("colA");
    final ConstantDimensionSelector colB = new ConstantDimensionSelector("colB");
    final ConstantDimensionSelector colC = new ConstantDimensionSelector("colC");
    final TestColumnSelectorFactory selectorFactory = new TestColumnSelectorFactory()
        .addDimSelector("colA", colA)
        .addDimSelector("colB", colB)
        .addDimSelector("colC", colC)
        .addCapabilities("colA", ColumnCapabilitiesImpl.createDefault())
        .addCapabilities("colB", ColumnCapabilitiesImpl.createDefault())
        .addCapabilities("colC", ColumnCapabilitiesImpl.createDefault());

    Assert.assertSame(colA, col.makeDimensionSelector(new IgnoredDimensionSpec(), selectorFactory));

    selectorFactory.addCapabilities("colA", null);
    Assert.assertSame(colB, col.makeDimensionSelector(new IgnoredDimensionSpec(), selectorFactory));

    selectorFactory.addCapabilities("colB", null);
    Assert.assertSame(colC, col.makeDimensionSelector(new IgnoredDimensionSpec(), selectorFactory));

    selectorFactory.addCapabilities("colC", null);
    Assert.assertSame(colA, col.makeDimensionSelector(new IgnoredDimensionSpec(), selectorFactory));
  }

  @Test
  public void testMakeColumnValueSelector()
  {
    final FallbackVirtualColumn col = makeCol("slimshady", "colA", "colB", "colC");

    final ConstantDimensionSelector colA = new ConstantDimensionSelector("colA");
    final ConstantDimensionSelector colB = new ConstantDimensionSelector("colB");
    final ConstantDimensionSelector colC = new ConstantDimensionSelector("colC");
    final TestColumnSelectorFactory selectorFactory = new TestColumnSelectorFactory()
        .addColumnSelector("colA", colA)
        .addColumnSelector("colB", colB)
        .addColumnSelector("colC", colC)
        .addCapabilities("colA", ColumnCapabilitiesImpl.createDefault())
        .addCapabilities("colB", ColumnCapabilitiesImpl.createDefault())
        .addCapabilities("colC", ColumnCapabilitiesImpl.createDefault());

    Assert.assertSame(colA, col.makeColumnValueSelector("abcd", selectorFactory));

    selectorFactory.addCapabilities("colA", null);
    Assert.assertSame(colB, col.makeColumnValueSelector("abcd", selectorFactory));

    selectorFactory.addCapabilities("colB", null);
    Assert.assertSame(colC, col.makeColumnValueSelector("abcd", selectorFactory));

    selectorFactory.addCapabilities("colC", null);
    Assert.assertSame(colA, col.makeColumnValueSelector("abcd", selectorFactory));
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testCanVectorize()
  {
    Assert.assertTrue(makeCol("slimshady", "test1").canVectorize(null));
  }

  @Test
  public void testMakeSingleValueVectorDimensionSelector()
  {
    final FallbackVirtualColumn col = makeCol("slimshady", "colA", "colB", "colC");

    final SameVectorSelector colA = new SameVectorSelector();
    final SameVectorSelector colB = new SameVectorSelector();
    final SameVectorSelector colC = new SameVectorSelector();
    final TestVectorColumnSelectorFactory selectorFactory = new TestVectorColumnSelectorFactory()
        .addSVDVS("colA", colA)
        .addSVDVS("colB", colB)
        .addSVDVS("colC", colC)
        .addCapabilities("colA", ColumnCapabilitiesImpl.createDefault())
        .addCapabilities("colB", ColumnCapabilitiesImpl.createDefault())
        .addCapabilities("colC", ColumnCapabilitiesImpl.createDefault());

    Assert.assertSame(colA, col.makeSingleValueVectorDimensionSelector(new IgnoredDimensionSpec(), selectorFactory));

    selectorFactory.addCapabilities("colA", null);
    Assert.assertSame(colB, col.makeSingleValueVectorDimensionSelector(new IgnoredDimensionSpec(), selectorFactory));

    selectorFactory.addCapabilities("colB", null);
    Assert.assertSame(colC, col.makeSingleValueVectorDimensionSelector(new IgnoredDimensionSpec(), selectorFactory));

    selectorFactory.addCapabilities("colC", null);
    Assert.assertSame(colA, col.makeSingleValueVectorDimensionSelector(new IgnoredDimensionSpec(), selectorFactory));
  }

  @Test
  public void testMakeMultiValueVectorDimensionSelector()
  {
    final FallbackVirtualColumn col = makeCol("slimshady", "colA", "colB", "colC");

    final SameMultiVectorSelector colA = new SameMultiVectorSelector();
    final SameMultiVectorSelector colB = new SameMultiVectorSelector();
    final SameMultiVectorSelector colC = new SameMultiVectorSelector();
    final TestVectorColumnSelectorFactory selectorFactory = new TestVectorColumnSelectorFactory()
        .addMVDVS("colA", colA)
        .addMVDVS("colB", colB)
        .addMVDVS("colC", colC)
        .addCapabilities("colA", ColumnCapabilitiesImpl.createDefault())
        .addCapabilities("colB", ColumnCapabilitiesImpl.createDefault())
        .addCapabilities("colC", ColumnCapabilitiesImpl.createDefault());

    Assert.assertSame(colA, col.makeMultiValueVectorDimensionSelector(new IgnoredDimensionSpec(), selectorFactory));

    selectorFactory.addCapabilities("colA", null);
    Assert.assertSame(colB, col.makeMultiValueVectorDimensionSelector(new IgnoredDimensionSpec(), selectorFactory));

    selectorFactory.addCapabilities("colB", null);
    Assert.assertSame(colC, col.makeMultiValueVectorDimensionSelector(new IgnoredDimensionSpec(), selectorFactory));

    selectorFactory.addCapabilities("colC", null);
    Assert.assertSame(colA, col.makeMultiValueVectorDimensionSelector(new IgnoredDimensionSpec(), selectorFactory));
  }

  @Test
  public void testMakeVectorValueSelector()
  {
    final FallbackVirtualColumn col = makeCol("slimshady", "colA", "colB", "colC");

    final SameVectorSelector colA = new SameVectorSelector();
    final SameVectorSelector colB = new SameVectorSelector();
    final SameVectorSelector colC = new SameVectorSelector();
    final TestVectorColumnSelectorFactory selectorFactory = new TestVectorColumnSelectorFactory()
        .addVVS("colA", colA)
        .addVVS("colB", colB)
        .addVVS("colC", colC)
        .addCapabilities("colA", ColumnCapabilitiesImpl.createDefault())
        .addCapabilities("colB", ColumnCapabilitiesImpl.createDefault())
        .addCapabilities("colC", ColumnCapabilitiesImpl.createDefault());

    Assert.assertSame(colA, col.makeVectorValueSelector("abcd", selectorFactory));

    selectorFactory.addCapabilities("colA", null);
    Assert.assertSame(colB, col.makeVectorValueSelector("abcd", selectorFactory));

    selectorFactory.addCapabilities("colB", null);
    Assert.assertSame(colC, col.makeVectorValueSelector("abcd", selectorFactory));

    selectorFactory.addCapabilities("colC", null);
    Assert.assertSame(colA, col.makeVectorValueSelector("abcd", selectorFactory));
  }

  @Test
  public void testMakeVectorObjectSelector()
  {
    final FallbackVirtualColumn col = makeCol("slimshady", "colA", "colB", "colC");

    final SameVectorSelector colA = new SameVectorSelector();
    final SameVectorSelector colB = new SameVectorSelector();
    final SameVectorSelector colC = new SameVectorSelector();
    final TestVectorColumnSelectorFactory selectorFactory = new TestVectorColumnSelectorFactory()
        .addVOS("colA", colA)
        .addVOS("colB", colB)
        .addVOS("colC", colC)
        .addCapabilities("colA", ColumnCapabilitiesImpl.createDefault())
        .addCapabilities("colB", ColumnCapabilitiesImpl.createDefault())
        .addCapabilities("colC", ColumnCapabilitiesImpl.createDefault());

    Assert.assertSame(colA, col.makeVectorObjectSelector("abcd", selectorFactory));

    selectorFactory.addCapabilities("colA", null);
    Assert.assertSame(colB, col.makeVectorObjectSelector("abcd", selectorFactory));

    selectorFactory.addCapabilities("colB", null);
    Assert.assertSame(colC, col.makeVectorObjectSelector("abcd", selectorFactory));

    selectorFactory.addCapabilities("colC", null);
    Assert.assertSame(colA, col.makeVectorObjectSelector("abcd", selectorFactory));
  }

  @Test
  public void testCapabilities()
  {
    final FallbackVirtualColumn col = makeCol("slimshady", "colA", "colB", "colC");

    final ColumnCapabilitiesImpl colA = ColumnCapabilitiesImpl.createDefault();
    final ColumnCapabilitiesImpl colB = ColumnCapabilitiesImpl.createDefault();
    final ColumnCapabilitiesImpl colC = ColumnCapabilitiesImpl.createDefault();
    final TestVectorColumnSelectorFactory selectorFactory = new TestVectorColumnSelectorFactory()
        .addCapabilities("colA", colA)
        .addCapabilities("colB", colB)
        .addCapabilities("colC", colC);

    Assert.assertEquals(ColumnCapabilitiesImpl.createDefault().getType(), col.capabilities("abcd").getType());

    Assert.assertSame(colA, col.capabilities(selectorFactory, "abcd"));

    selectorFactory.addCapabilities("colA", null);
    Assert.assertSame(colB, col.capabilities(selectorFactory, "abcd"));

    selectorFactory.addCapabilities("colB", null);
    Assert.assertSame(colC, col.capabilities(selectorFactory, "abcd"));

    selectorFactory.addCapabilities("colC", null);
    Assert.assertNull(col.capabilities(selectorFactory, "abcd"));
  }

  @Test
  public void testRequiredColumns()
  {
    Assert.assertEquals(
        Arrays.asList("colA", "colB", "oneMore"),
        makeCol("slimshady", "colA", "colB", "oneMore").requiredColumns()
    );
  }

  @Test
  public void testUsesDotNotation()
  {
    Assert.assertFalse(makeCol("hi", "my", "name", "is").usesDotNotation());
  }

  @Test
  public void testGetIndexSupplier()
  {
    final FallbackVirtualColumn col = makeCol("slimshady", "colA", "colB", "colC");

    final SameColumnIndexSupplier colA = new SameColumnIndexSupplier();
    final SameColumnIndexSupplier colB = new SameColumnIndexSupplier();
    final SameColumnIndexSupplier colC = new SameColumnIndexSupplier();
    final TestColumnSelector selectorFactory = new TestColumnSelector()
        .addHolder("colA", new HolderForIndexSupplier(colA))
        .addHolder("colB", new HolderForIndexSupplier(colB))
        .addHolder("colC", new HolderForIndexSupplier(colC))
        .addCapabilities("colA", ColumnCapabilitiesImpl.createDefault())
        .addCapabilities("colB", ColumnCapabilitiesImpl.createDefault())
        .addCapabilities("colC", ColumnCapabilitiesImpl.createDefault());
    final ColumnSelectorColumnIndexSelector columnIndexSelector = new ColumnSelectorColumnIndexSelector(
        RoaringBitmapFactory.INSTANCE,
        VirtualColumns.EMPTY,
        selectorFactory
    );

    Assert.assertSame(colA, col.getIndexSupplier("abcd", columnIndexSelector));

    selectorFactory.addCapabilities("colA", null);
    Assert.assertSame(colB, col.getIndexSupplier("abcd", columnIndexSelector));

    selectorFactory.addCapabilities("colB", null);
    Assert.assertSame(colC, col.getIndexSupplier("abcd", columnIndexSelector));

    selectorFactory.addCapabilities("colC", null);
    Assert.assertSame(colA, col.getIndexSupplier("abcd", columnIndexSelector));

  }

  private static FallbackVirtualColumn makeCol(String name, String... cols)
  {
    return makeCol(name, Arrays.stream(cols).map(DefaultDimensionSpec::of).toArray(DimensionSpec[]::new));
  }

  private static FallbackVirtualColumn makeCol(String name, DimensionSpec... specs)
  {
    return new FallbackVirtualColumn(name, new ArrayList<>(Arrays.asList(specs)));
  }

  private static class IgnoredDimensionSpec implements DimensionSpec
  {

    @Override
    public byte[] getCacheKey()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getDimension()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getOutputName()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public ColumnType getOutputType()
    {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public ExtractionFn getExtractionFn()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public DimensionSelector decorate(DimensionSelector selector)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean mustDecorate()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean preservesOrdering()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public DimensionSpec withDimension(String newDimension)
    {
      throw new UnsupportedOperationException();
    }
  }

  private static class SameVectorSelector implements SingleValueDimensionVectorSelector, VectorValueSelector,
      VectorObjectSelector
  {
    @Override
    public int[] getRowVector()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getValueCardinality()
    {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxVectorSize()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getCurrentVectorSize()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public long[] getLongVector()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[] getFloatVector()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public double[] getDoubleVector()
    {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public boolean[] getNullVector()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object[] getObjectVector()
    {
      throw new UnsupportedOperationException();
    }
  }

  private static class SameColumnIndexSupplier implements ColumnIndexSupplier
  {
    @Nullable
    @Override
    public <T> T as(Class<T> clazz)
    {
      throw new UnsupportedOperationException();
    }
  }

  public static class SameMultiVectorSelector implements MultiValueDimensionVectorSelector
  {
    @Override
    public int getValueCardinality()
    {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public IndexedInts[] getRowVector()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxVectorSize()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getCurrentVectorSize()
    {
      throw new UnsupportedOperationException();
    }
  }

  private static class HolderForIndexSupplier implements ColumnHolder
  {
    private final ColumnIndexSupplier indexSupplier;

    public HolderForIndexSupplier(
        ColumnIndexSupplier indexSupplier
    )
    {
      this.indexSupplier = indexSupplier;
    }

    @Override
    public ColumnCapabilities getCapabilities()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getLength()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public BaseColumn getColumn()
    {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public ColumnIndexSupplier getIndexSupplier()
    {
      return indexSupplier;
    }

    @Override
    public SettableColumnValueSelector<?> makeNewSettableColumnValueSelector()
    {
      throw new UnsupportedOperationException();
    }
  }
}
