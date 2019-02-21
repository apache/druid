/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexer.hadoop;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.indexer.JobHelper;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.realtime.firehose.IngestSegmentFirehose;
import org.apache.druid.segment.realtime.firehose.WindowedStorageAdapter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DatasourceRecordReader extends RecordReader<NullWritable, InputRow>
{
  private static final Logger logger = new Logger(DatasourceRecordReader.class);

  private DatasourceIngestionSpec spec;
  private IngestSegmentFirehose firehose;

  private long rowNum;
  private Row currRow;

  private List<QueryableIndex> indexes = new ArrayList<>();
  private List<File> tmpSegmentDirs = new ArrayList<>();
  private long numRows;

  @Override
  public void initialize(InputSplit split, final TaskAttemptContext context) throws IOException
  {
    List<WindowedDataSegment> segments = ((DatasourceInputSplit) split).getSegments();
    String dataSource = Iterators.getOnlyElement(
        segments.stream()
                .map(s -> s.getSegment().getDataSource())
                .distinct()
                .iterator()
    );

    spec = DatasourceInputFormat.getIngestionSpec(context.getConfiguration(), dataSource);
    logger.info("load schema [%s]", spec);

    List<WindowedStorageAdapter> adapters = Lists.transform(
        segments,
        new Function<WindowedDataSegment, WindowedStorageAdapter>()
        {
          @Override
          public WindowedStorageAdapter apply(WindowedDataSegment segment)
          {
            try {
              logger.info("Getting storage path for segment [%s]", segment.getSegment().getId());
              Path path = new Path(JobHelper.getURIFromSegment(segment.getSegment()));

              logger.info("Fetch segment files from [%s]", path);

              File dir = Files.createTempDir();
              tmpSegmentDirs.add(dir);
              logger.info("Locally storing fetched segment at [%s]", dir);

              JobHelper.unzipNoGuava(path, context.getConfiguration(), dir, context, null);
              logger.info("finished fetching segment files");

              QueryableIndex index = HadoopDruidIndexerConfig.INDEX_IO.loadIndex(dir);
              indexes.add(index);
              numRows += index.getNumRows();

              return new WindowedStorageAdapter(
                  new QueryableIndexStorageAdapter(index),
                  segment.getInterval()
              );
            }
            catch (IOException ex) {
              throw new RuntimeException(ex);
            }
          }
        }
    );

    firehose = new IngestSegmentFirehose(
        adapters,
        spec.getTransformSpec(),
        spec.getDimensions(),
        spec.getMetrics(),
        spec.getFilter()
    );
  }

  @Override
  public boolean nextKeyValue()
  {
    if (firehose.hasMore()) {
      currRow = firehose.nextRow();
      rowNum++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public NullWritable getCurrentKey()
  {
    return NullWritable.get();
  }

  @Override
  public InputRow getCurrentValue()
  {
    return currRow == null ? null : new SegmentInputRow(currRow, spec.getDimensions());
  }

  @Override
  public float getProgress()
  {
    if (numRows > 0) {
      return (rowNum * 1.0f) / numRows;
    } else {
      return 0;
    }
  }

  @Override
  public void close() throws IOException
  {
    Closeables.close(firehose, true);
    for (QueryableIndex qi : indexes) {
      Closeables.close(qi, true);
    }

    for (File dir : tmpSegmentDirs) {
      FileUtils.deleteDirectory(dir);
    }
  }
}
