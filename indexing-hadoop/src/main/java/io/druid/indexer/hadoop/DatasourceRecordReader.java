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

package io.druid.indexer.hadoop;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.JobHelper;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.realtime.firehose.IngestSegmentFirehose;
import io.druid.segment.realtime.firehose.WindowedStorageAdapter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class DatasourceRecordReader extends RecordReader<NullWritable, InputRow>
{
  private static final Logger logger = new Logger(DatasourceRecordReader.class);

  private DatasourceIngestionSpec spec;
  private IngestSegmentFirehose firehose;

  private int rowNum;
  private MapBasedRow currRow;

  private List<QueryableIndex> indexes = Lists.newArrayList();
  private List<File> tmpSegmentDirs = Lists.newArrayList();
  private int numRows;

  @Override
  public void initialize(InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException
  {
    spec = readAndVerifyDatasourceIngestionSpec(context.getConfiguration(), HadoopDruidIndexerConfig.JSON_MAPPER);

    List<WindowedDataSegment> segments = ((DatasourceInputSplit) split).getSegments();

    List<WindowedStorageAdapter> adapters = Lists.transform(
        segments,
        new Function<WindowedDataSegment, WindowedStorageAdapter>()
        {
          @Override
          public WindowedStorageAdapter apply(WindowedDataSegment segment)
          {
            try {
              logger.info("Getting storage path for segment [%s]", segment.getSegment().getIdentifier());
              Path path = new Path(JobHelper.getURIFromSegment(segment.getSegment()));

              logger.info("Fetch segment files from [%s]", path);

              File dir = Files.createTempDir();
              tmpSegmentDirs.add(dir);
              logger.info("Locally storing fetched segment at [%s]", dir);

              JobHelper.unzipNoGuava(path, context.getConfiguration(), dir, context);
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
              throw Throwables.propagate(ex);
            }
          }
        }
    );

    firehose = new IngestSegmentFirehose(
        adapters,
        spec.getDimensions(),
        spec.getMetrics(),
        spec.getFilter()
    );

  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    if (firehose.hasMore()) {
      currRow = (MapBasedRow) firehose.nextRow();
      rowNum++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException
  {
    return NullWritable.get();
  }

  @Override
  public InputRow getCurrentValue() throws IOException, InterruptedException
  {
    return new SegmentInputRow(
        new MapBasedInputRow(
            currRow.getTimestamp(),
            spec.getDimensions(),
            currRow.getEvent()
        )
    );
  }

  @Override
  public float getProgress() throws IOException, InterruptedException
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

  private DatasourceIngestionSpec readAndVerifyDatasourceIngestionSpec(Configuration config, ObjectMapper jsonMapper)
  {
    try {
      String schema = Preconditions.checkNotNull(config.get(DatasourceInputFormat.CONF_DRUID_SCHEMA), "null schema");
      logger.info("load schema [%s]", schema);

      DatasourceIngestionSpec spec = jsonMapper.readValue(schema, DatasourceIngestionSpec.class);

      if (spec.getDimensions() == null || spec.getDimensions().size() == 0) {
        throw new ISE("load schema does not have dimensions");
      }

      if (spec.getMetrics() == null || spec.getMetrics().size() == 0) {
        throw new ISE("load schema does not have metrics");
      }

      return spec;
    }
    catch (IOException ex) {
      throw new RuntimeException("couldn't load segment load spec", ex);
    }
  }
}
