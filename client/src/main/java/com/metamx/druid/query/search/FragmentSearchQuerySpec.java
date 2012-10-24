package com.metamx.druid.query.search;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class FragmentSearchQuerySpec implements SearchQuerySpec
{
  private static final byte CACHE_TYPE_ID = 0x2;

  private final List<String> values;
  private final SearchSortSpec sortSpec;

  @JsonCreator
  public FragmentSearchQuerySpec(
      @JsonProperty("values") List<String> values,
      @JsonProperty("sort") SearchSortSpec sortSpec
  )
  {
    this.values = Lists.transform(
        values,
        new Function<String, String>()
        {
          @Override
          public String apply(String s)
          {
            return s.toLowerCase();
          }
        }
    );
    this.sortSpec = (sortSpec == null) ? new LexicographicSearchSortSpec() : sortSpec;
  }

  @JsonProperty
  public List<String> getValues()
  {
    return values;
  }

  @JsonProperty("sort")
  @Override
  public SearchSortSpec getSearchSortSpec()
  {
    return sortSpec;
  }

  @Override
  public boolean accept(String dimVal)
  {
    for (String value : values) {
      if (!dimVal.toLowerCase().contains(value)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[][] valuesBytes = new byte[values.size()][];
    int valuesBytesSize = 0;
    int index = 0;
    for (String value : values) {
      valuesBytes[index] = value.getBytes();
      valuesBytesSize += valuesBytes[index].length;
      ++index;
    }

    final ByteBuffer queryCacheKey = ByteBuffer.allocate(1 + valuesBytesSize)
                                               .put(CACHE_TYPE_ID);

    for (byte[] bytes : valuesBytes) {
      queryCacheKey.put(bytes);
    }

    return queryCacheKey.array();
  }

  @Override
  public String toString()
  {
    return "FragmentSearchQuerySpec{" +
             "values=" + values +
             ", sortSpec=" + sortSpec +
           "}";
  }
}
