package io.druid.indexer.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.indexer.path.PathSpec;
import io.druid.indexer.updater.DbUpdaterJobSpec;
import io.druid.segment.indexing.IOConfig;

/**
 */
@JsonTypeName("batch")
public class BatchIOConfig implements IOConfig
{
  private final PathSpec pathSpec;
  private final DbUpdaterJobSpec updaterJobSpec;

  @JsonCreator
  public BatchIOConfig(
      @JsonProperty("pathSpec") PathSpec pathSpec,
      @JsonProperty("updaterJobSpec") DbUpdaterJobSpec updaterJobSpec
  )
  {
    this.pathSpec = pathSpec;
    this.updaterJobSpec = updaterJobSpec;
  }

  @JsonProperty
  public PathSpec getPathSpec()
  {
    return pathSpec;
  }

  @JsonProperty
  public DbUpdaterJobSpec getUpdaterJobSpec()
  {
    return updaterJobSpec;
  }
}
