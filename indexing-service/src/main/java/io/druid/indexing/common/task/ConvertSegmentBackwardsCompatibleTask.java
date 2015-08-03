package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.segment.IndexSpec;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

@Deprecated
public class ConvertSegmentBackwardsCompatibleTask extends ConvertSegmentTask
{
  @JsonCreator
  public ConvertSegmentBackwardsCompatibleTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("segment") DataSegment segment,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      @JsonProperty("force") Boolean force,
      @JsonProperty("validate") Boolean validate
  )
  {
    super(
        id == null ? ConvertSegmentTask.makeId(dataSource, interval) : id,
        dataSource,
        interval,
        segment,
        indexSpec,
        force == null ? false : force,
        validate ==null ? false : validate,
        null
    );
  }

  @Deprecated
  public static class SubTask extends ConvertSegmentTask.SubTask
  {
    @JsonCreator
    public SubTask(
        @JsonProperty("groupId") String groupId,
        @JsonProperty("segment") DataSegment segment,
        @JsonProperty("indexSpec") IndexSpec indexSpec,
        @JsonProperty("force") Boolean force,
        @JsonProperty("validate") Boolean validate
    )
    {
      super(groupId, segment, indexSpec, force, validate, null);
    }
  }
}
