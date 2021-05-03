package org.apache.druid.indexing.overlord.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.DatasourceIntervals;

import java.util.Map;

/**
 * Response of API /lockedIntervals
 */
public class LockedIntervalsResponse
{
  private final Map<String, DatasourceIntervals> lockedIntervals;

  @JsonCreator
  public LockedIntervalsResponse(
      @JsonProperty("lockedIntervals") Map<String, DatasourceIntervals> lockedIntervals
  )
  {
    this.lockedIntervals = lockedIntervals;
  }

  @JsonProperty("lockedIntervals")
  public Map<String, DatasourceIntervals> getLockedIntervals()
  {
    return lockedIntervals;
  }
}
