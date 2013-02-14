package com.metamx.druid.merger.common.task;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.actions.LockListAction;
import com.metamx.druid.merger.common.actions.SegmentListUnusedAction;
import com.metamx.druid.merger.common.actions.SegmentNukeAction;
import com.metamx.druid.merger.common.TaskLock;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;

/**
 */
public class KillTask extends AbstractTask
{
  private static final Logger log = new Logger(KillTask.class);

  @JsonCreator
  public KillTask(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval
  )
  {
    super(
        String.format(
            "kill_%s_%s_%s_%s",
            dataSource,
            interval.getStart(),
            interval.getEnd(),
            new DateTime().toString()
        ),
        dataSource,
        interval
    );
  }

  @Override
  public String getType()
  {
    return "kill";
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    // Confirm we have a lock (will throw if there isn't exactly one element)
    final TaskLock myLock = Iterables.getOnlyElement(toolbox.getTaskActionClient().submit(new LockListAction(this)));

    if(!myLock.getDataSource().equals(getDataSource())) {
      throw new ISE("WTF?! Lock dataSource[%s] != task dataSource[%s]", myLock.getDataSource(), getDataSource());
    }

    if(!myLock.getInterval().equals(getFixedInterval().get())) {
      throw new ISE("WTF?! Lock interval[%s] != task interval[%s]", myLock.getInterval(), getFixedInterval().get());
    }

    // List unused segments
    final List<DataSegment> unusedSegments = toolbox.getTaskActionClient()
                                                    .submit(
                                                        new SegmentListUnusedAction(
                                                            this,
                                                            myLock.getDataSource(),
                                                            myLock.getInterval()
                                                        )
                                                    );

    // Verify none of these segments have versions > lock version
    for(final DataSegment unusedSegment : unusedSegments) {
      if(unusedSegment.getVersion().compareTo(myLock.getVersion()) > 0) {
        throw new ISE(
            "WTF?! Unused segment[%s] has version[%s] > task version[%s]",
            unusedSegment.getIdentifier(),
            unusedSegment.getVersion(),
            myLock.getVersion()
        );
      }

      log.info("OK to kill segment: %s", unusedSegment.getIdentifier());
    }

    // Kill segments
    toolbox.getSegmentKiller().kill(unusedSegments);

    // Remove metadata for these segments
    toolbox.getTaskActionClient().submit(new SegmentNukeAction(this, ImmutableSet.copyOf(unusedSegments)));

    return TaskStatus.success(getId());
  }
}
