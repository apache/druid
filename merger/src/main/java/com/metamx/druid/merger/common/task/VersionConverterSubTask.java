/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.merger.common.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.actions.SegmentInsertAction;
import org.joda.time.DateTime;

import java.io.File;
import java.util.Arrays;
import java.util.Map;

/**
 */
public class VersionConverterSubTask extends AbstractTask
{
  private static final Logger log = new Logger(VersionConverterSubTask.class);

  private final DataSegment segment;

  protected VersionConverterSubTask(
      @JsonProperty("groupId") String groupId,
      @JsonProperty("segment") DataSegment segment
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
        segment.getInterval()
    );
    this.segment = segment;
  }

  @Override
  public String getType()
  {
    return "version_converter_sub";
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    log.info("Converting segment[%s]", segment);
    final Map<DataSegment, File> localSegments = toolbox.getSegments(Arrays.asList(segment));

    final File location = localSegments.get(segment);
    final File outLocation = new File(location, "v9_out");
    if (IndexIO.convertSegment(location, outLocation)) {
      final int outVersion = IndexIO.getVersionFromDir(outLocation);

      // Appending to the version makes a new version that inherits most comparability parameters of the original
      // version, but is "newer" than said original version.
      DataSegment updatedSegment = segment.withVersion(String.format("%s_v%s", segment.getVersion(), outVersion));
      updatedSegment = toolbox.getSegmentPusher().push(outLocation, updatedSegment);

      toolbox.getTaskActionClientFactory().submit(new SegmentInsertAction(Sets.newHashSet(updatedSegment)));
    }
    else {
      log.info("Conversion failed.");
    }

    return success();
  }
}
