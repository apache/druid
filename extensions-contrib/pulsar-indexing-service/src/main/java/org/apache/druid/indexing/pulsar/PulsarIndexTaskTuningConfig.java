package org.apache.druid.indexing.pulsar;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Period;

import javax.annotation.Nullable;

import java.io.File;

@JsonTypeName("PulsarTuningConfig")
public class PulsarIndexTaskTuningConfig extends SeekableStreamIndexTaskTuningConfig
{

  @JsonCreator
  public PulsarIndexTaskTuningConfig(
      @JsonProperty("appendableIndexSpec") @Nullable AppendableIndexSpec appendableIndexSpec,
      @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
      @JsonProperty("skipBytesInMemoryOverheadCheck") @Nullable Boolean skipBytesInMemoryOverheadCheck,
      @JsonProperty("maxRowsPerSegment") @Nullable Integer maxRowsPerSegment,
      @JsonProperty("maxTotalRows") @Nullable Long maxTotalRows,
      @JsonProperty("intermediatePersistPeriod") @Nullable Period intermediatePersistPeriod,
      @JsonProperty("basePersistDirectory") @Nullable File basePersistDirectory,
      @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
      @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
      @JsonProperty("indexSpecForIntermediatePersists") @Nullable IndexSpec indexSpecForIntermediatePersists,
      // This parameter is left for compatibility when reading existing configs, to be removed in Druid 0.12.
      @Deprecated @JsonProperty("reportParseExceptions") @Nullable Boolean reportParseExceptions,
      @JsonProperty("handoffConditionTimeout") @Nullable Long handoffConditionTimeout,
      @JsonProperty("resetOffsetAutomatically") @Nullable Boolean resetOffsetAutomatically,
      @JsonProperty("skipSequenceNumberAvailabilityCheck") @Nullable Boolean skipSequenceNumberAvailabilityCheck,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("intermediateHandoffPeriod") @Nullable Period intermediateHandoffPeriod,
      @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions)
  {
    super(appendableIndexSpec, maxRowsInMemory, maxBytesInMemory, skipBytesInMemoryOverheadCheck, maxRowsPerSegment,
        maxTotalRows, intermediatePersistPeriod, basePersistDirectory, maxPendingPersists, indexSpec,
        indexSpecForIntermediatePersists, reportParseExceptions, handoffConditionTimeout, resetOffsetAutomatically,
        skipSequenceNumberAvailabilityCheck, segmentWriteOutMediumFactory, intermediateHandoffPeriod,
        logParseExceptions,
        maxParseExceptions, maxSavedParseExceptions);
  }

  @Override
  public SeekableStreamIndexTaskTuningConfig withBasePersistDirectory(File dir)
  {
    return new PulsarIndexTaskTuningConfig(
        getAppendableIndexSpec(),
        getMaxRowsInMemory(),
        getMaxBytesInMemory(),
        isSkipBytesInMemoryOverheadCheck(),
        getMaxRowsPerSegment(),
        getMaxTotalRows(),
        getIntermediatePersistPeriod(),
        dir,
        getMaxPendingPersists(),
        getIndexSpec(),
        getIndexSpecForIntermediatePersists(),
        isReportParseExceptions(),
        getHandoffConditionTimeout(),
        isResetOffsetAutomatically(),
        isSkipSequenceNumberAvailabilityCheck(),
        getSegmentWriteOutMediumFactory(),
        getIntermediateHandoffPeriod(),
        isLogParseExceptions(),
        getMaxParseExceptions(),
        getMaxSavedParseExceptions()
    );
  }

  @Override
  public String toString()
  {
    return "PulsarIndexTaskTuningConfig{" +
           "maxRowsInMemory=" + getMaxRowsInMemory() +
           ", maxRowsPerSegment=" + getMaxRowsPerSegment() +
           ", maxTotalRows=" + getMaxTotalRows() +
           ", maxBytesInMemory=" + getMaxBytesInMemory() +
           ", skipBytesInMemoryOverheadCheck=" + isSkipBytesInMemoryOverheadCheck() +
           ", intermediatePersistPeriod=" + getIntermediatePersistPeriod() +
           ", basePersistDirectory=" + getBasePersistDirectory() +
           ", maxPendingPersists=" + getMaxPendingPersists() +
           ", indexSpec=" + getIndexSpec() +
           ", indexSpecForIntermediatePersists=" + getIndexSpecForIntermediatePersists() +
           ", reportParseExceptions=" + isReportParseExceptions() +
           ", handoffConditionTimeout=" + getHandoffConditionTimeout() +
           ", resetOffsetAutomatically=" + isResetOffsetAutomatically() +
           ", segmentWriteOutMediumFactory=" + getSegmentWriteOutMediumFactory() +
           ", intermediateHandoffPeriod=" + getIntermediateHandoffPeriod() +
           ", logParseExceptions=" + isLogParseExceptions() +
           ", maxParseExceptions=" + getMaxParseExceptions() +
           ", maxSavedParseExceptions=" + getMaxSavedParseExceptions() +
           '}';
  }
}
