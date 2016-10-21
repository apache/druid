/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentInsertAction;
import io.druid.indexing.common.actions.SegmentListUsedAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexSpec;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This task takes a segment and attempts to reindex it in the latest version with the specified indexSpec.
 * <p/>
 * Only datasource must be specified. `indexSpec` and `force` are highly suggested but optional. The rest get
 * auto-configured and should only be modified with great care
 */
public class ConvertSegmentTask extends AbstractFixedIntervalTask
{
  private static final String TYPE = "convert_segment";
  private static final Integer CURR_VERSION_INTEGER = IndexIO.CURRENT_VERSION_ID;

  private static final Logger log = new Logger(ConvertSegmentTask.class);

  @JsonIgnore
  private final DataSegment segment;
  private final IndexSpec indexSpec;
  private final boolean force;
  private final boolean validate;

  /**
   * Create a segment converter task to convert a segment to the most recent version including the specified indexSpec
   *
   * @param dataSource The datasource to which this update should be applied
   * @param interval   The interval in the datasource which to apply the update to
   * @param indexSpec  The IndexSpec to use in the updated segments
   * @param force      Force an update, even if the task thinks it doesn't need to update.
   * @param validate   Validate the new segment compared to the old segment on a row by row basis
   *
   * @return A SegmentConverterTask for the datasource's interval with the indexSpec specified.
   */
  public static ConvertSegmentTask create(
      String dataSource,
      Interval interval,
      IndexSpec indexSpec,
      boolean force,
      boolean validate,
      Map<String, Object> context
  )
  {
    final String id = makeId(dataSource, interval);
    return new ConvertSegmentTask(id, dataSource, interval, null, indexSpec, force, validate, context);
  }

  /**
   * Create a task to update the segment specified to the most recent binary version with the specified indexSpec
   *
   * @param segment   The segment to which this update should be applied
   * @param indexSpec The IndexSpec to use in the updated segments
   * @param force     Force an update, even if the task thinks it doesn't need to update.
   * @param validate  Validate the new segment compared to the old segment on a row by row basis
   *
   * @return A SegmentConverterTask for the segment with the indexSpec specified.
   */
  public static ConvertSegmentTask create(
      DataSegment segment,
      IndexSpec indexSpec,
      boolean force,
      boolean validate,
      Map<String, Object> context
  )
  {
    final Interval interval = segment.getInterval();
    final String dataSource = segment.getDataSource();
    final String id = makeId(dataSource, interval);
    return new ConvertSegmentTask(id, dataSource, interval, segment, indexSpec, force, validate, context);
  }

  protected static String makeId(String dataSource, Interval interval)
  {
    Preconditions.checkNotNull(dataSource, "dataSource");
    Preconditions.checkNotNull(interval, "interval");
    return joinId(TYPE, dataSource, interval.getStart(), interval.getEnd(), new DateTime());
  }

  @JsonCreator
  private static ConvertSegmentTask createFromJson(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("segment") DataSegment segment,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      @JsonProperty("force") Boolean force,
      @JsonProperty("validate") Boolean validate,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    final boolean isForce = force == null ? false : force;
    final boolean isValidate = validate == null ? true : validate;
    if (id == null) {
      if (segment == null) {
        return create(dataSource, interval, indexSpec, isForce, isValidate, context);
      } else {
        return create(segment, indexSpec, isForce, isValidate, context);
      }
    }
    return new ConvertSegmentTask(id, dataSource, interval, segment, indexSpec, isForce, isValidate, context);
  }

  protected ConvertSegmentTask(
      String id,
      String dataSource,
      Interval interval,
      DataSegment segment,
      IndexSpec indexSpec,
      boolean force,
      boolean validate,
      Map<String, Object> context
  )
  {
    super(id, dataSource, interval, context);
    this.segment = segment;
    this.indexSpec = indexSpec == null ? new IndexSpec() : indexSpec;
    this.force = force;
    this.validate = validate;
  }

  @JsonProperty
  public boolean isForce()
  {
    return force;
  }

  @JsonProperty
  public boolean isValidate()
  {
    return validate;
  }

  @JsonProperty
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @JsonProperty
  public DataSegment getSegment()
  {
    return segment;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    final Iterable<DataSegment> segmentsToUpdate;
    if (segment == null) {
      final List<DataSegment> segments = toolbox.getTaskActionClient().submit(
          new SegmentListUsedAction(
              getDataSource(),
              getInterval(),
              null
          )
      );
      segmentsToUpdate = FunctionalIterable
          .create(segments)
          .filter(
              new Predicate<DataSegment>()
              {
                @Override
                public boolean apply(DataSegment segment)
                {
                  final Integer segmentVersion = segment.getBinaryVersion();
                  if (!CURR_VERSION_INTEGER.equals(segmentVersion)) {
                    return true;
                  } else if (force) {
                    log.info(
                        "Segment[%s] already at version[%s], forcing conversion",
                        segment.getIdentifier(),
                        segmentVersion
                    );
                    return true;
                  } else {
                    log.info("Skipping[%s], already version[%s]", segment.getIdentifier(), segmentVersion);
                    return false;
                  }
                }
              }
          );
    } else {
      log.info("I'm in a subless mood.");
      segmentsToUpdate = Collections.singleton(segment);
    }
    // Vestigial from a past time when this task spawned subtasks.
    for (final Task subTask : generateSubTasks(getGroupId(), segmentsToUpdate, indexSpec, force, validate, getContext())) {
      final TaskStatus status = subTask.run(toolbox);
      if (!status.isSuccess()) {
        return TaskStatus.fromCode(getId(), status.getStatusCode());
      }
    }
    return success();
  }

  protected Iterable<Task> generateSubTasks(
      final String groupId,
      final Iterable<DataSegment> segments,
      final IndexSpec indexSpec,
      final boolean force,
      final boolean validate,
      final Map<String, Object> context
  )
  {
    return Iterables.transform(
        segments,
        new Function<DataSegment, Task>()
        {
          @Override
          public Task apply(DataSegment input)
          {
            return new SubTask(groupId, input, indexSpec, force, validate, context);
          }
        }
    );
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

    ConvertSegmentTask that = (ConvertSegmentTask) o;

    if (segment != null ? !segment.equals(that.segment) : that.segment != null) {
      return false;
    }

    return super.equals(o);
  }

  public static class SubTask extends AbstractFixedIntervalTask
  {
    @JsonIgnore
    private final DataSegment segment;
    private final IndexSpec indexSpec;
    private final boolean force;
    private final boolean validate;

    @JsonCreator
    public SubTask(
        @JsonProperty("groupId") String groupId,
        @JsonProperty("segment") DataSegment segment,
        @JsonProperty("indexSpec") IndexSpec indexSpec,
        @JsonProperty("force") Boolean force,
        @JsonProperty("validate") Boolean validate,
        @JsonProperty("context") Map<String, Object> context
    )
    {
      super(
          joinId(
              groupId,
              "sub",
              segment.getInterval().getStart(),
              segment.getInterval().getEnd(),
              segment.getShardSpec().getPartitionNum()
          ),
          groupId,
          segment.getDataSource(),
          segment.getInterval(),
          context
      );
      this.segment = segment;
      this.indexSpec = indexSpec == null ? new IndexSpec() : indexSpec;
      this.force = force == null ? false : force;
      this.validate = validate == null ? true : validate;
    }

    @JsonProperty
    public boolean isValidate()
    {
      return validate;
    }

    @JsonProperty
    public boolean isForce()
    {
      return force;
    }

    @JsonProperty
    public DataSegment getSegment()
    {
      return segment;
    }

    @Override
    public String getType()
    {
      return "version_converter_sub";
    }

    @Override
    public TaskStatus run(TaskToolbox toolbox) throws Exception
    {
      log.info("Subs are good!  Italian BMT and Meatball are probably my favorite.");
      convertSegment(toolbox, segment, indexSpec, force, validate);
      return success();
    }
  }

  private static void convertSegment(
      TaskToolbox toolbox,
      final DataSegment segment,
      IndexSpec indexSpec,
      boolean force,
      boolean validate
  )
      throws SegmentLoadingException, IOException
  {
    log.info("Converting segment[%s]", segment);
    final TaskActionClient actionClient = toolbox.getTaskActionClient();
    final List<DataSegment> currentSegments = actionClient.submit(
        new SegmentListUsedAction(segment.getDataSource(), segment.getInterval(), null)
    );

    for (DataSegment currentSegment : currentSegments) {
      final String version = currentSegment.getVersion();
      final Integer binaryVersion = currentSegment.getBinaryVersion();

      if (!force && (version.startsWith(segment.getVersion()) && CURR_VERSION_INTEGER.equals(binaryVersion))) {
        log.info("Skipping already updated segment[%s].", segment);
        return;
      }
    }

    final Map<DataSegment, File> localSegments = toolbox.fetchSegments(Collections.singletonList(segment));

    final File location = localSegments.get(segment);
    final File outLocation = new File(location, "v9_out");
    if (toolbox.getIndexIO().convertSegment(location, outLocation, indexSpec, force, validate)) {
      final int outVersion = IndexIO.getVersionFromDir(outLocation);

      // Appending to the version makes a new version that inherits most comparability parameters of the original
      // version, but is "newer" than said original version.
      DataSegment updatedSegment = segment.withVersion(String.format("%s_v%s", segment.getVersion(), outVersion));
      updatedSegment = toolbox.getSegmentPusher().push(outLocation, updatedSegment);

      actionClient.submit(new SegmentInsertAction(Sets.newHashSet(updatedSegment)));
    } else {
      log.info("Conversion failed.");
    }
  }
}
