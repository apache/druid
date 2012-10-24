package com.metamx.druid.merger.coordinator;

import com.metamx.druid.client.DataSegment;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Set;

/**
 * Information gathered by the coordinator, after acquiring a lock, that may be useful to a task.
 */
public class TaskContext
{
  final String version;
  final Set<DataSegment> currentSegments;

  @JsonCreator
  public TaskContext(
      @JsonProperty("version") String version,
      @JsonProperty("currentSegments") Set<DataSegment> currentSegments
  )
  {
    this.version = version;
    this.currentSegments = currentSegments;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  @JsonProperty
  public Set<DataSegment> getCurrentSegments()
  {
    return currentSegments;
  }
}
