package com.metamx.druid.merger.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.indexer.HadoopDruidIndexerConfig;
import com.metamx.druid.indexer.HadoopDruidIndexerJob;
import com.metamx.druid.loading.S3DataSegmentPusher;
import com.metamx.druid.merger.common.TaskLock;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.actions.LockListAction;
import com.metamx.druid.merger.common.actions.SegmentInsertAction;
import com.metamx.druid.utils.JodaUtils;
import org.joda.time.DateTime;

import java.util.List;

public class HadoopIndexTask extends AbstractTask
{
  @JsonProperty
  private final HadoopDruidIndexerConfig config;

  private static final Logger log = new Logger(HadoopIndexTask.class);

  /**
   * @param config is used by the HadoopDruidIndexerJob to set up the appropriate parameters
   *               for creating Druid index segments. It may be modified.
   *               <p/>
   *               Here, we will ensure that the UpdaterJobSpec field of the config is set to null, such that the
   *               job does not push a list of published segments the database. Instead, we will use the method
   *               IndexGeneratorJob.getPublishedSegments() to simply return a list of the published
   *               segments, and let the indexing service report these segments to the database.
   */

  @JsonCreator
  public HadoopIndexTask(
      @JsonProperty("config") HadoopDruidIndexerConfig config
  )
  {
    super(
        String.format("index_hadoop_%s_%s", config.getDataSource(), new DateTime()),
        config.getDataSource(),
        JodaUtils.umbrellaInterval(config.getIntervals())
    );

    // Some HadoopDruidIndexerConfig stuff doesn't make sense in the context of the indexing service
    Preconditions.checkArgument(config.getSegmentOutputDir() == null, "segmentOutputPath must be absent");
    Preconditions.checkArgument(config.getJobOutputDir() == null, "workingPath must be absent");
    Preconditions.checkArgument(!config.isUpdaterJobSpecSet(), "updaterJobSpec must be absent");

    this.config = config;
  }

  @Override
  public String getType()
  {
    return "index_hadoop";
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    // Copy config so we don't needlessly modify our provided one
    // Also necessary to make constructor validations work upon serde-after-run
    final HadoopDruidIndexerConfig configCopy = toolbox.getObjectMapper()
                                                       .readValue(
                                                           toolbox.getObjectMapper().writeValueAsBytes(config),
                                                           HadoopDruidIndexerConfig.class
                                                       );

    // We should have a lock from before we started running
    final TaskLock myLock = Iterables.getOnlyElement(toolbox.getTaskActionClient().submit(new LockListAction(this)));
    log.info("Setting version to: %s", myLock.getVersion());
    configCopy.setVersion(myLock.getVersion());

    // Set workingPath to some reasonable default
    configCopy.setJobOutputDir(toolbox.getConfig().getHadoopWorkingPath());

    if (toolbox.getSegmentPusher() instanceof S3DataSegmentPusher) {
      // Hack alert! Bypassing DataSegmentPusher...
      S3DataSegmentPusher segmentPusher = (S3DataSegmentPusher) toolbox.getSegmentPusher();
      String s3Path = String.format(
          "s3n://%s/%s/%s",
          segmentPusher.getConfig().getBucket(),
          segmentPusher.getConfig().getBaseKey(),
          getDataSource()
      );

      log.info("Setting segment output path to: %s", s3Path);
      configCopy.setSegmentOutputDir(s3Path);
    } else {
      throw new IllegalStateException("Sorry, we only work with S3DataSegmentPushers! Bummer!");
    }

    HadoopDruidIndexerJob job = new HadoopDruidIndexerJob(configCopy);
    configCopy.verify();

    log.info("Starting a hadoop index generator job...");
    if (job.run()) {
      List<DataSegment> publishedSegments = job.getPublishedSegments();

      // Request segment pushes
      toolbox.getTaskActionClient().submit(new SegmentInsertAction(this, ImmutableSet.copyOf(publishedSegments)));

      // Done
      return TaskStatus.success(getId());
    } else {
      return TaskStatus.failure(getId());
    }

  }
}
