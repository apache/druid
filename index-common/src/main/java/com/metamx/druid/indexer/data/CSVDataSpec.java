package com.metamx.druid.indexer.data;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.common.parsers.CSVParser;
import com.metamx.common.parsers.Parser;
import com.metamx.common.parsers.ToLowerCaseParser;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

/**
 */
public class CSVDataSpec implements DataSpec
{
  private final List<String> columns;
  private final List<String> dimensions;

  @JsonCreator
  public CSVDataSpec(
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("dimensions") List<String> dimensions
  )
  {
    Preconditions.checkNotNull(columns);
    Preconditions.checkArgument(
        !Joiner.on("_").join(columns).contains(","), "Columns must not have commas in them"
    );

    this.columns = Lists.transform(
        columns,
        new Function<String, String>()
        {
          @Override
          public String apply(@Nullable String input)
          {
            return input.toLowerCase();
          }
        }
    );
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

  @JsonProperty("columns")
  public List<String> getColumns()
  {
    return columns;
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
    for (String columnName : usedCols) {
      Preconditions.checkArgument(columns.contains(columnName), "column[%s] not in columns.", columnName);
    }
  }

  @Override
  public boolean hasCustomDimensions()
  {
    return !(dimensions == null || dimensions.isEmpty());
  }

  @Override
  public Parser getParser()
  {
    return new ToLowerCaseParser(new CSVParser(columns));
  }
}
