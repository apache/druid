package org.apache.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.IngestionState;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

public class ParallelCompactionTaskReportData extends IngestionStatsAndErrorsTaskReportData
{
  @JsonProperty
  private Long segmentsRead;
  @JsonProperty
  private Long segmentsPublished;

  @JsonProperty
  private Map<String, Long> recordsProcessed;

  public ParallelCompactionTaskReportData(
      @JsonProperty("ingestionState") IngestionState ingestionState,
      @JsonProperty("unparseableEvents") Map<String, Object> unparseableEvents,
      @JsonProperty("rowStats") Map<String, Object> rowStats,
      @JsonProperty("errorMsg") @Nullable String errorMsg,
      @JsonProperty("segmentAvailabilityConfirmed") boolean segmentAvailabilityConfirmed,
      @JsonProperty("segmentAvailabilityWaitTimeMs") long segmentAvailabilityWaitTimeMs,
      @JsonProperty("recordsProcessed") Map<String, Long> recordsProcessed,
      @JsonProperty("segmentsRead") Long segmentsRead,
      @JsonProperty("segmentsPublished") Long segmentsPublished
  )
  {
    super(
        ingestionState,
        unparseableEvents,
        rowStats,
        errorMsg,
        segmentAvailabilityConfirmed,
        segmentAvailabilityWaitTimeMs,
        recordsProcessed
    );
    this.segmentsRead = segmentsRead;
    this.segmentsPublished = segmentsPublished;
  }

  @JsonProperty
  @Nullable
  public Long getSegmentsRead()
  {
    return segmentsRead;
  }

  @JsonProperty
  @Nullable
  public Long getSegmentsPublished()
  {
    return segmentsPublished;
  }

  public static IngestionStatsAndErrorsTaskReportData getPayloadFromTaskReports(
      Map<String, TaskReport> taskReports
  )
  {
    return (IngestionStatsAndErrorsTaskReportData) taskReports.get(IngestionStatsAndErrorsTaskReport.REPORT_KEY)
                                                              .getPayload();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ParallelCompactionTaskReportData that = (ParallelCompactionTaskReportData) o;
    return super.equals(o) &&
           Objects.equals(getSegmentsRead(), that.getSegmentsRead()) &&
           Objects.equals(getSegmentsPublished(), that.getSegmentsPublished());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        getSegmentsRead(),
        getSegmentsPublished()
    );
  }

  @Override
  public String toString()
  {
    return "ParallelCompactionTaskReportData {" +
           "IngestionStatsAndErrorsTaskReportData=" + super.toString() +
           ", segmentsRead=" + segmentsRead +
           ", segmentsPublished=" + segmentsPublished +
           '}';
  }
}
