package org.apache.druid.data.input.impl.delta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.delta.kernel.data.Row;

import java.util.List;

public class DeltaSplit
{
  private final String stateRow;
  private final List<String> fileRows;

  @JsonCreator
  public DeltaSplit(@JsonProperty("state") String stateRow, @JsonProperty("file") List<String> fileRows)
  {
    this.stateRow = stateRow;
    this.fileRows = fileRows;
  }

  @JsonProperty("state")
  public String getStateRow()
  {
    return stateRow;
  }

  @JsonProperty("file")
  public List<String> getFile()
  {
    return fileRows;
  }


  @Override
  public String toString()
  {
     return "DeltaSplit{" +
            "stateRow=" + stateRow +
            ", file=" + fileRows +
            "}";
  }
}
