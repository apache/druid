package com.metamx.druid.indexer.data;

import com.metamx.common.parsers.Parser;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.util.List;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format", defaultImpl = DelimitedDataSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "json", value = JSONDataSpec.class),
    @JsonSubTypes.Type(name = "csv", value = CSVDataSpec.class),
    @JsonSubTypes.Type(name = "tsv", value = DelimitedDataSpec.class)
})
public interface DataSpec
{
  public void verify(List<String> usedCols);

  public boolean hasCustomDimensions();

  public List<String> getDimensions();

  public Parser<String, Object> getParser();
}
