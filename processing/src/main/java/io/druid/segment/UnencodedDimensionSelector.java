package io.druid.segment;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.IAE;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;

import java.util.List;
import java.util.Map;

public class UnencodedDimensionSelector implements DimensionSelector
{
  private final LongColumnSelector longSelector;
  private final FloatColumnSelector floatSelector;
  private final ExtractionFn extractionFn;
  private final ValueType type;
  private final ColumnCapabilities capabilities;
  private final Map<Comparable, String> remappedValues = Maps.newHashMap();

  public UnencodedDimensionSelector(Object selector, ExtractionFn extractionFn, ColumnCapabilities capabilities)
  {
    if (selector instanceof LongColumnSelector) {
      type = ValueType.LONG;
      this.longSelector = (LongColumnSelector) selector;
      this.floatSelector = null;
    } else if (selector instanceof FloatColumnSelector) {
      type = ValueType.FLOAT;
      this.longSelector = null;
      this.floatSelector = (FloatColumnSelector) selector;
    } else {
      throw new IAE("unencoded column only supports Long and Float ColumnSelectors");
    }

    this.extractionFn = extractionFn;
    this.capabilities = capabilities;
  }

  @Override
  public IndexedInts getRow()
  {
    throw new UnsupportedOperationException("unencoded column does not support getRow");
  }

  @Override
  public int getValueCardinality()
  {
    return Integer.MAX_VALUE;
  }

  @Override
  public int lookupId(String name)
  {
    throw new UnsupportedOperationException("unencoded column does not support lookupId");
  }

  @Override
  public String lookupName(int id)
  {
    throw new UnsupportedOperationException("unencoded column does not support lookupName");
  }

  @Override
  public List<Comparable> getUnencodedRow()
  {
    List<Comparable> rowVals = Lists.newArrayList();
    switch (type) {
      case LONG:
        rowVals.add(longSelector.get());
        break;
      case FLOAT:
        rowVals.add(floatSelector.get());
        break;
      default:
        break;
    }
    return rowVals;
  }

  @Override
  public Comparable getExtractedValueFromUnencoded(Comparable rowVal)
  {
    if (extractionFn == null) {
      return rowVal;
    }

    String extractedVal = remappedValues.get(rowVal);
    if (extractedVal == null) {
      extractedVal = extractionFn.apply(rowVal);
      remappedValues.put(rowVal, extractedVal);
    }
    return extractedVal;
  }

  @Override
  public ColumnCapabilities getDimCapabilities()
  {
    return capabilities;
  }

}
