package com.metamx.druid.query.group;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class GroupByQueryConfig
{
  @JsonProperty
  private boolean singleThreaded = false;

  @JsonProperty
  private int maxIntermediateRows = 50000;

  @JsonProperty
  private int maxResults = 500000;

  public boolean isSingleThreaded()
  {
    return singleThreaded;
  }

  public int getMaxIntermediateRows()
  {
    return maxIntermediateRows;
  }

  public void setMaxIntermediateRows(int maxIntermediateRows)
  {
    this.maxIntermediateRows = maxIntermediateRows;
  }

  public int getMaxResults()
  {
    return maxResults;
  }
}
