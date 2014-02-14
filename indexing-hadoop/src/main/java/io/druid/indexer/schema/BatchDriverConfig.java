package io.druid.indexer.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.segment.indexing.DriverConfig;

/**
 */
@JsonTypeName("batch")
public class BatchDriverConfig implements DriverConfig
{
  private final String workingPath;
  private final String segmentOutputPath;
  private final String version;
  private final PartitionsSpec partitionsSpec;
  private final boolean leaveIntermediate;
  private final boolean cleanupOnFailure;
  private final boolean overwriteFiles;
  private final boolean ignoreInvalidRows;

  @JsonCreator
  public BatchDriverConfig(
      final @JsonProperty("workingPath") String workingPath,
      final @JsonProperty("segmentOutputPath") String segmentOutputPath,
      final @JsonProperty("version") String version,
      final @JsonProperty("partitionsSpec") PartitionsSpec partitionsSpec,
      final @JsonProperty("leaveIntermediate") boolean leaveIntermediate,
      final @JsonProperty("cleanupOnFailure") Boolean cleanupOnFailure,
      final @JsonProperty("overwriteFiles") boolean overwriteFiles,
      final @JsonProperty("ignoreInvalidRows") boolean ignoreInvalidRows
  )
  {
    this.workingPath = workingPath;
    this.segmentOutputPath = segmentOutputPath;
    this.version = version;
    this.partitionsSpec = partitionsSpec;
    this.leaveIntermediate = leaveIntermediate;
    this.cleanupOnFailure = (cleanupOnFailure == null) ? true : cleanupOnFailure;
    this.overwriteFiles = overwriteFiles;
    this.ignoreInvalidRows = ignoreInvalidRows;
  }

  @JsonProperty
  public String getWorkingPath()
  {
    return workingPath;
  }

  @JsonProperty
  public String getSegmentOutputPath()
  {
    return segmentOutputPath;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  @JsonProperty
  public PartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
  }

  @JsonProperty
  public boolean isLeaveIntermediate()
  {
    return leaveIntermediate;
  }

  @JsonProperty
  public boolean isCleanupOnFailure()
  {
    return cleanupOnFailure;
  }

  @JsonProperty
  public boolean isOverwriteFiles()
  {
    return overwriteFiles;
  }

  @JsonProperty
  public boolean isIgnoreInvalidRows()
  {
    return ignoreInvalidRows;
  }
}
