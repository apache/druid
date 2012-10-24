package com.metamx.druid.realtime;

import com.google.common.base.Preconditions;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.Period;

/**
 */
public class FireDepartmentConfig
{
  private final int maxRowsInMemory;
  private final Period intermediatePersistPeriod;

  public FireDepartmentConfig(
      @JsonProperty("maxRowsInMemory") int maxRowsInMemory,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod
  )
  {
    this.maxRowsInMemory = maxRowsInMemory;
    this.intermediatePersistPeriod = intermediatePersistPeriod;

    Preconditions.checkArgument(maxRowsInMemory > 0, "maxRowsInMemory[%s] should be greater than 0", maxRowsInMemory);
    Preconditions.checkNotNull(intermediatePersistPeriod, "intermediatePersistPeriod");
  }

  @JsonProperty
  public int getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  @JsonProperty
  public Period getIntermediatePersistPeriod()
  {
    return intermediatePersistPeriod;
  }
}
