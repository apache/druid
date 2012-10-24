package com.metamx.druid.indexer.data;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.parsers.JSONParser;
import com.metamx.common.parsers.Parser;
import com.metamx.common.parsers.ToLowerCaseParser;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

/**
 */
public class JSONDataSpec implements DataSpec
{
  private final List<String> dimensions;

  public JSONDataSpec(
      @JsonProperty("dimensions") List<String> dimensions
  )
  {
    this.dimensions = (dimensions == null) ? dimensions : Lists.transform(
        dimensions,
        new Function<String, String>()
        {
          @Override
          public String apply(@Nullable String input)
          {
            return input.toLowerCase();
          }
        }
    );
  }

  @JsonProperty("dimensions")
  @Override
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @Override
  public void verify(List<String> usedCols)
  {
  }

  @Override
  public boolean hasCustomDimensions()
  {
    return !(dimensions == null || dimensions.isEmpty());
  }

  @Override
  public Parser getParser()
  {
    return new ToLowerCaseParser(new JSONParser());
  }
}
