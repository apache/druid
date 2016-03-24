package io.druid.segment;

import com.google.common.collect.Maps;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.IndexedFloats;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedFloats;
import io.druid.segment.data.IndexedLongs;

import java.io.IOException;
import java.util.Map;

public class FloatDimensionSelector implements DimensionSelector
{
  private final FloatColumnSelector selector;
  private final ExtractionFn extractionFn;
  private final ColumnCapabilities capabilities;
  private final Map<Comparable, String> remappedValues = Maps.newHashMap();

  public FloatDimensionSelector(Object selector, ExtractionFn extractionFn, ColumnCapabilities capabilities)
  {
    this.selector = (FloatColumnSelector) selector;
    this.extractionFn = extractionFn;
    this.capabilities = capabilities;
  }

  @Override
  public IndexedInts getRow()
  {
    throw new UnsupportedOperationException("float column does not support getRow");
  }

  @Override
  public int getValueCardinality()
  {
    return Integer.MAX_VALUE;
  }

  @Override
  public int lookupId(String name)
  {
    throw new UnsupportedOperationException("float column does not support lookupId");
  }

  @Override
  public String lookupName(int id)
  {
    throw new UnsupportedOperationException("float column does not support lookupName");
  }

  @Override
  public IndexedFloats getFloatRow()
  {
    final float val = selector.get();
    return new IndexedFloats()
    {
      @Override
      public int size()
      {
        // only single-value numerics are supported
        return 1;
      }

      @Override
      public float get(int index)
      {
        return val;
      }

      @Override
      public void fill(int index, float[] toFill)
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
  public Comparable getExtractedValueFloat(float val)
  {
    if (extractionFn == null) {
      return val;
    }

    String extractedVal = remappedValues.get(val);
    if (extractedVal == null) {
      extractedVal = extractionFn.apply(val);
      remappedValues.put(val, extractedVal);
    }
    return extractedVal;
  }

  @Override
  public IndexedLongs getLongRow()
  {
    throw new UnsupportedOperationException("float column does not support getFloatRow");
  }

  @Override
  public Comparable getExtractedValueLong(long val)
  {
    throw new UnsupportedOperationException("float column does not support getExtractedValueFloat");
  }

  @Override
  public Comparable getComparableRow()
  {
    throw new UnsupportedOperationException("float column does not support getComparableRow");
  }

  @Override
  public Comparable getExtractedValueComparable(Comparable val)
  {
    throw new UnsupportedOperationException("float column does not support getExtractedValueComparable");
  }

  @Override
  public ColumnCapabilities getDimCapabilities()
  {
    return capabilities;
  }

}
