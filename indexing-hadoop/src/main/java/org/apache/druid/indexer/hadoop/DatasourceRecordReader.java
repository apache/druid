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
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.indexer.JobHelper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.realtime.WindowedCursorFactory;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.transform.Transformer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DatasourceRecordReader extends RecordReader<NullWritable, InputRow>
{
  private static final Logger logger = new Logger(DatasourceRecordReader.class);

  private DatasourceIngestionSpec spec;
  private SegmentReader segmentReader;

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

    List<WindowedCursorFactory> adapters = Lists.transform(
        segments,
        new Function<>()
        {
          @Override
          public WindowedCursorFactory apply(WindowedDataSegment segment)
          {
            try {
              logger.info("Getting storage path for segment [%s]", segment.getSegment().getId());
              Path path = new Path(JobHelper.getURIFromSegment(segment.getSegment()));

              logger.info("Fetch segment files from [%s]", path);

              File dir = FileUtils.createTempDir();
              tmpSegmentDirs.add(dir);
              logger.info("Locally storing fetched segment at [%s]", dir);

              JobHelper.unzipNoGuava(path, context.getConfiguration(), dir, context, null);
              logger.info("finished fetching segment files");

              QueryableIndex index = HadoopDruidIndexerConfig.INDEX_IO.loadIndex(dir);
              indexes.add(index);
              numRows += index.getNumRows();

              return new WindowedCursorFactory(new QueryableIndexCursorFactory(index), segment.getInterval());
            }
            catch (IOException ex) {
              throw new RuntimeException(ex);
            }
          }
        }
    );

    segmentReader = new SegmentReader(
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
    if (segmentReader.hasMore()) {
      currRow = segmentReader.nextRow();
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
    Closeables.close(segmentReader, true);
    for (QueryableIndex qi : indexes) {
      Closeables.close(qi, true);
    }

    for (File dir : tmpSegmentDirs) {
      FileUtils.deleteDirectory(dir);
    }
  }

  public static class SegmentReader implements Closeable
  {
    private final Transformer transformer;
    private Yielder<InputRow> rowYielder;

    public SegmentReader(
        final List<WindowedCursorFactory> cursorFactories,
        final TransformSpec transformSpec,
        final List<String> dims,
        final List<String> metrics,
        final DimFilter dimFilter
    )
    {
      this.transformer = transformSpec.toTransformer();

      Sequence<InputRow> rows = Sequences.concat(
          Iterables.transform(
              cursorFactories,
              new Function<>()
              {
                @Nullable
                @Override
                public Sequence<InputRow> apply(WindowedCursorFactory windowed)
                {
                  final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                                   .setFilter(Filters.toFilter(dimFilter))
                                                                   .setInterval(windowed.getInterval())
                                                                   .build();
                  final CursorHolder cursorHolder = windowed.getCursorFactory().makeCursorHolder(buildSpec);
                  final Cursor cursor = cursorHolder.asCursor();
                  if (cursor == null) {
                    return Sequences.empty();
                  }
                  final BaseLongColumnValueSelector timestampColumnSelector =
                      cursor.getColumnSelectorFactory().makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

                  final Map<String, DimensionSelector> dimSelectors = new HashMap<>();
                  for (String dim : dims) {
                    final DimensionSelector dimSelector = cursor
                        .getColumnSelectorFactory()
                        .makeDimensionSelector(new DefaultDimensionSpec(dim, dim));
                    // dimSelector is null if the dimension is not present
                    if (dimSelector != null) {
                      dimSelectors.put(dim, dimSelector);
                    }
                  }

                  final Map<String, BaseObjectColumnValueSelector> metSelectors = new HashMap<>();
                  for (String metric : metrics) {
                    final BaseObjectColumnValueSelector metricSelector =
                        cursor.getColumnSelectorFactory().makeColumnValueSelector(metric);
                    metSelectors.put(metric, metricSelector);
                  }

                  return Sequences.simple(
                      new Iterable<InputRow>()
                      {
                        @Override
                        public Iterator<InputRow> iterator()
                        {
                          return new Iterator<>()
                          {
                            @Override
                            public boolean hasNext()
                            {
                              return !cursor.isDone();
                            }

                            @Override
                            public InputRow next()
                            {
                              final Map<String, Object> theEvent = Maps.newLinkedHashMap();
                              final long timestamp = timestampColumnSelector.getLong();
                              theEvent.put(TimestampSpec.DEFAULT_COLUMN, DateTimes.utc(timestamp));

                              for (Map.Entry<String, DimensionSelector> dimSelector :
                                  dimSelectors.entrySet()) {
                                final String dim = dimSelector.getKey();
                                final DimensionSelector selector = dimSelector.getValue();
                                final IndexedInts vals = selector.getRow();

                                int valsSize = vals.size();
                                if (valsSize == 1) {
                                  final String dimVal = selector.lookupName(vals.get(0));
                                  theEvent.put(dim, dimVal);
                                } else if (valsSize > 1) {
                                  List<String> dimVals = new ArrayList<>(valsSize);
                                  for (int i = 0; i < valsSize; ++i) {
                                    dimVals.add(selector.lookupName(vals.get(i)));
                                  }
                                  theEvent.put(dim, dimVals);
                                }
                              }

                              for (Map.Entry<String, BaseObjectColumnValueSelector> metSelector :
                                  metSelectors.entrySet()) {
                                final String metric = metSelector.getKey();
                                final BaseObjectColumnValueSelector selector = metSelector.getValue();
                                Object value = selector.getObject();
                                if (value != null) {
                                  theEvent.put(metric, value);
                                }
                              }
                              cursor.advance();
                              return new MapBasedInputRow(timestamp, dims, theEvent);
                            }

                            @Override
                            public void remove()
                            {
                              throw new UnsupportedOperationException("Remove Not Supported");
                            }
                          };
                        }
                      }
                  ).withBaggage(cursorHolder);
                }
              }
          )
      );
      rowYielder = Yielders.each(rows);
    }

    public boolean hasMore()
    {
      return !rowYielder.isDone();
    }

    @Nullable
    public InputRow nextRow()
    {
      final InputRow inputRow = rowYielder.get();
      rowYielder = rowYielder.next(null);
      return transformer.transform(inputRow);
    }

    @Override
    public void close() throws IOException
    {
      rowYielder.close();
    }
  }
}
