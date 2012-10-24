package com.metamx.druid.merger.common.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.indexer.granularity.GranularitySpec;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.coordinator.TaskContext;
import com.metamx.druid.realtime.FirehoseFactory;
import com.metamx.druid.realtime.Schema;
import com.metamx.druid.shard.NoneShardSpec;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;

public class IndexTask extends AbstractTask
{
  @JsonProperty private final GranularitySpec granularitySpec;
  @JsonProperty private final AggregatorFactory[] aggregators;
  @JsonProperty private final QueryGranularity indexGranularity;
  @JsonProperty private final long targetPartitionSize;
  @JsonProperty private final FirehoseFactory firehoseFactory;

  private static final Logger log = new Logger(IndexTask.class);

  @JsonCreator
  public IndexTask(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("granularitySpec") GranularitySpec granularitySpec,
      @JsonProperty("aggregators") AggregatorFactory[] aggregators,
      @JsonProperty("indexGranularity") QueryGranularity indexGranularity,
      @JsonProperty("targetPartitionSize") long targetPartitionSize,
      @JsonProperty("firehose") FirehoseFactory firehoseFactory
  )
  {
    super(
        // _not_ the version, just something uniqueish
        String.format("index_%s_%s", dataSource, new DateTime().toString()),
        dataSource,
        new Interval(
            granularitySpec.bucketIntervals().first().getStart(),
            granularitySpec.bucketIntervals().last().getEnd()
        )
    );

    this.granularitySpec = Preconditions.checkNotNull(granularitySpec, "granularitySpec");
    this.aggregators = aggregators;
    this.indexGranularity = indexGranularity;
    this.targetPartitionSize = targetPartitionSize;
    this.firehoseFactory = firehoseFactory;
  }

  public List<Task> toSubtasks()
  {
    final List<Task> retVal = Lists.newArrayList();

    for (final Interval interval : granularitySpec.bucketIntervals()) {
      if (targetPartitionSize > 0) {
        // Need to do one pass over the data before indexing in order to determine good partitions
        retVal.add(
            new IndexDeterminePartitionsTask(
                getGroupId(),
                interval,
                firehoseFactory,
                new Schema(
                    getDataSource(),
                    aggregators,
                    indexGranularity,
                    new NoneShardSpec()
                ),
                targetPartitionSize
            )
        );
      } else {
        // Jump straight into indexing
        retVal.add(
            new IndexGeneratorTask(
                getGroupId(),
                interval,
                firehoseFactory,
                new Schema(
                    getDataSource(),
                    aggregators,
                    indexGranularity,
                    new NoneShardSpec()
                )
            )
        );
      }
    }

    return retVal;
  }

  @Override
  public Type getType()
  {
    return Type.INDEX;
  }

  @Override
  public TaskStatus preflight(TaskContext context) throws Exception
  {
    return TaskStatus.continued(getId(), toSubtasks());
  }

  @Override
  public TaskStatus run(TaskContext context, TaskToolbox toolbox) throws Exception
  {
    throw new IllegalStateException("IndexTasks should not be run!");
  }
}
