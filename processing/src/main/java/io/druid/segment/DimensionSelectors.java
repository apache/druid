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

package io.druid.segment;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.metamx.common.Pair;
import io.druid.data.input.Row;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.column.Column;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.Offset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DimensionSelectors
{
  public static DimensionSelector makeMultiDimensionalSelectorFromIndex(
      final DimensionSpec dimensionSpec,
      final Function<Pair<String, DimensionSpec>, DimensionSelector> selectorGenerator
  )
  {
    List<String> dimensions = dimensionSpec.getDimensions();
    if (dimensions.size() == 1) {
      return selectorGenerator.apply(new Pair<>(dimensions.get(0), dimensionSpec));
    } else {
      final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();
      Preconditions.checkArgument(extractionFn != null && (extractionFn.arity() < 0 || dimensions.size() == extractionFn.arity()),
          "number of dimensions between extractionFn() and input dimensions mismatch!");

      List<DimensionSelector> selectors = Lists.transform(
          dimensions,
          new Function<String, DimensionSelector>()
          {
            @Override
            public DimensionSelector apply(String dimension)
            {
              return selectorGenerator.apply(
                  new Pair<>(
                      dimension,
                      getDimensionSpecWithoutExtractionFn(dimensionSpec)
                  )
              );
            }
          }
      );

      return DimensionSelectors.makeMultiDimensionalSelector(
          selectors,
          extractionFn
      );
    }
  }

  public static DimensionSelector makeSelectorFromQueryableIndex(
      final DictionaryEncodedColumn<String> column,
      final Column columnDesc,
      final Supplier<Offset> offsetSupplier,
      final ExtractionFn extractionFn
  )
  {
    if (columnDesc.getCapabilities().hasMultipleValues()) {
      return new DimensionSelector()
      {
        @Override
        public IndexedInts getRow()
        {
          return column.getMultiValueRow(offsetSupplier.get().getOffset());
        }

        @Override
        public int getValueCardinality()
        {
          return column.getCardinality();
        }

        @Override
        public String lookupName(int id)
        {
          final String value = column.lookupName(id);
          return extractionFn == null ?
              value :
              extractionFn.apply(value);
        }

        @Override
        public int lookupId(String name)
        {
          if (extractionFn != null) {
            throw new UnsupportedOperationException(
                "cannot perform lookup when applying an extraction function"
            );
          }
          return column.lookupId(name);
        }
      };
    } else {
      return new DimensionSelector()
      {
        @Override
        public IndexedInts getRow()
        {
          // using an anonymous class is faster than creating a class that stores a copy of the value
          return new IndexedInts()
          {
            @Override
            public int size()
            {
              return 1;
            }

            @Override
            public int get(int index)
            {
              return column.getSingleValueRow(offsetSupplier.get().getOffset());
            }

            @Override
            public Iterator<Integer> iterator()
            {
              return Iterators.singletonIterator(column.getSingleValueRow(offsetSupplier.get().getOffset()));
            }

            @Override
            public void fill(int index, int[] toFill)
            {
              throw new UnsupportedOperationException("fill not supported");
            }

            @Override
            public void close() throws IOException
            {

            }
          };
        }

        @Override
        public int getValueCardinality()
        {
          return column.getCardinality();
        }

        @Override
        public String lookupName(int id)
        {
          final String value = column.lookupName(id);
          return extractionFn == null ? value : extractionFn.apply(value);
        }

        @Override
        public int lookupId(String name)
        {
          if (extractionFn != null) {
            throw new UnsupportedOperationException(
                "cannot perform lookup when applying an extraction function"
            );
          }
          return column.lookupId(name);
        }
      };
    }
  }

  public static DimensionSelector makeMultiDimensionalSelectorFromRow(
      final Supplier<Row> rowSupplier,
      final DimensionSpec dimensionSpec
  )
  {
    final List<String> dimensions = dimensionSpec.getDimensions();
    final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

    if (dimensions.size() == 1) {
      return makeSelectorFromRow(
          rowSupplier,
          dimensions.get(0),
          extractionFn
      );
    } else {
      List<DimensionSelector> selectors = Lists.transform(
          dimensions,
          new Function<String, DimensionSelector>()
          {
            @Override
            public DimensionSelector apply(String dimension)
            {
              return DimensionSelectors.makeSelectorFromRow(
                  rowSupplier,
                  dimension,
                  null
              );
            }
          }
      );

      return DimensionSelectors.makeMultiDimensionalSelector(
          selectors,
          extractionFn
      );
    }
  }

  public static DimensionSelector makeSelectorFromRow(
      final Supplier<Row> rowSupplier,
      final String dimension,
      final ExtractionFn extractionFn
  )
  {
    return new DimensionSelector()
    {
      @Override
      public IndexedInts getRow()
      {
        final List<String> dimensionValues = rowSupplier.get().getDimension(dimension);
        final ArrayList<Integer> vals = Lists.newArrayList();
        if (dimensionValues != null) {
          for (int i = 0; i < dimensionValues.size(); ++i) {
            vals.add(i);
          }
        }

        return new IndexedInts()
        {
          @Override
          public int size()
          {
            return vals.size();
          }

          @Override
          public int get(int index)
          {
            return vals.get(index);
          }

          @Override
          public Iterator<Integer> iterator()
          {
            return vals.iterator();
          }

          @Override
          public void close() throws IOException
          {

          }

          @Override
          public void fill(int index, int[] toFill)
          {
            throw new UnsupportedOperationException("fill not supported");
          }
        };
      }

      @Override
      public int getValueCardinality()
      {
        return DimensionSelector.CARDINALITY_UNKNOWN;
      }

      @Override
      public String lookupName(int id)
      {
        final String value = rowSupplier.get().getDimension(dimension).get(id);
        return extractionFn == null ? value : extractionFn.apply(value);
      }

      @Override
      public int lookupId(String name)
      {
        if (extractionFn != null) {
          throw new UnsupportedOperationException("cannot perform lookup when applying an extraction function");
        }
        return rowSupplier.get().getDimension(dimension).indexOf(name);
      }
    };
  }

  public static DimensionSelector makeMultiDimensionalSelector(
      final List<DimensionSelector> selectors,
      final ExtractionFn extractionFn
  )
  {
    Preconditions.checkArgument(extractionFn != null && (extractionFn.arity() < 0 || extractionFn.arity() == selectors.size()),
        "ExtractionFn should be specified and have the same arity as the number of dimensions");

    return new DimensionSelector()
    {
      StringDimensionIndexer.DimensionDictionary dimLookup =
          new StringDimensionIndexer.DimensionDictionary();

      @Override
      public IndexedInts getRow()
      {
        final List<IndexedInts> rows = Lists.transform(
            selectors,
            new Function<DimensionSelector, IndexedInts>()
            {
              @Override
              public IndexedInts apply(DimensionSelector selector)
              {
                return selector.getRow();
              }
            }
        );

        int size = 1;
        for (IndexedInts row: rows) {
          size *= row.size();
        }
        final int finalSize = size;

        final List<Integer> vals = Lists.newArrayListWithCapacity(size);

        for (int idx = 0; idx < size; idx++) {
          List<String> args = Lists.newArrayListWithCapacity(rows.size());
          int index = idx;
          int indexInRow;
          for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++)
          {
            DimensionSelector selector = selectors.get(rowIdx);
            IndexedInts row = rows.get(rowIdx);
            if (index == 0) {
              indexInRow = 0;
            } else {
              indexInRow = index % row.size();
              index = index / row.size();
            }
            args.add(selector.lookupName(row.get(indexInRow)));
          }
          vals.add(dimLookup.add(extractionFn.apply(args)));
        }

        return new IndexedInts()
        {
          @Override
          public int size()
          {
            return finalSize;
          }

          @Override
          public int get(int index)
          {
            return vals.get(index);
          }

          @Override
          public void fill(int index, int[] toFill)
          {
            throw new UnsupportedOperationException("fill not supported");
          }

          @Override
          public void close() throws IOException
          {

          }

          @Override
          public Iterator<Integer> iterator()
          {
            return vals.iterator();
          }
        };
      }

      @Override
      public int getValueCardinality()
      {
        // dictionary is not supported for multi-dimensional dimensionSpec
        return DimensionSelector.CARDINALITY_UNKNOWN;
      }

      @Override
      public String lookupName(int id)
      {
        return dimLookup.getValue(id);
      }

      @Override
      public int lookupId(String name)
      {
        throw new UnsupportedOperationException(
            "cannot perform lookup when applying an extraction function"
        );
      }
    };
  }

  private static DimensionSpec getDimensionSpecWithoutExtractionFn(final DimensionSpec dimensionSpec)
  {
    return new DimensionSpec()
    {
      @Override
      public List<String> getDimensions()
      {
        return dimensionSpec.getDimensions();
      }

      @Override
      public String getOutputName()
      {
        return dimensionSpec.getOutputName();
      }

      @Override
      public ExtractionFn getExtractionFn()
      {
        return null;
      }

      @Override
      public DimensionSelector decorate(DimensionSelector selector)
      {
        return dimensionSpec.decorate(selector);
      }

      @Override
      public byte[] getCacheKey()
      {
        return dimensionSpec.getCacheKey();
      }

      @Override
      public boolean preservesOrdering()
      {
        return dimensionSpec.preservesOrdering();
      }
    };
  }
}
