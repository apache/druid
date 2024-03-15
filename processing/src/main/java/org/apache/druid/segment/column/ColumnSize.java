package org.apache.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;

public class ColumnSize
{
  public static ColumnSize NO_DATA = new ColumnSize(0, new LinkedHashMap<>());


  public static final String DATA_SECTION = "data";
  public static final String LONG_COLUMN_PART = "longValuesColumn";
  public static final String DOUBLE_COLUMN_PART = "doubleValuesColumn";
  public static final String ENCODED_VALUE_COLUMN_PART = "encodedValueColumn";
  public static final String STRING_VALUE_DICTIONARY_COLUMN_PART = "stringValueDictionary";
  public static final String LONG_VALUE_DICTIONARY_COLUMN_PART = "longValueDictionary";
  public static final String DOUBLE_VALUE_DICTIONARY_COLUMN_PART = "doubleValueDictionary";
  public static final String ARRAY_VALUE_DICTIONARY_COLUMN_PART = "arrayValueDictionary";
  public static final String NULL_VALUE_INDEX_COLUMN_PART = "nullValueIndex";
  public static final String BITMAP_VALUE_INDEX_COLUMN_PART = "bitmapValueIndexes";
  public static final String BITMAP_ARRAY_ELEMENT_INDEX_COLUMN_PART = "bitmapArrayElementIndexes";

  private final long size;
  private final LinkedHashMap<String, ColumnPartSize> components;

  @JsonCreator
  public ColumnSize(
      @JsonProperty("size") long size,
      @JsonProperty("components") LinkedHashMap<String, ColumnPartSize> components)
  {
    this.size = size;
    this.components = components;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public long getSize()
  {
    return size;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public LinkedHashMap<String, ColumnPartSize> getComponents()
  {
    return components;
  }

  public ColumnSize merge(@Nullable ColumnSize other) {
    if (other == null) {
      return this;
    }
    long newSize = size + other.size;
    LinkedHashMap<String, ColumnPartSize> combined = new LinkedHashMap<>(components);
    for (Map.Entry<String, ColumnPartSize> sizes : other.getComponents().entrySet()) {
      combined.compute(sizes.getKey(), (k, componentSize) -> {
        if (componentSize == null) {
          return sizes.getValue();
        }
        return componentSize.merge(sizes.getValue());
      });
    }
    return new ColumnSize(newSize, combined);
  }

  @Override
  public String toString()
  {
    return "ColumnSize{" +
           "size=" + size +
           ", components=" + components +
           '}';
  }
}
