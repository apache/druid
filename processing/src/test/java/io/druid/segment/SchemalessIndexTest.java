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

package io.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.MapBasedInputRow;
import io.druid.hll.HyperLogLogHash;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.writeout.SegmentWriteOutMediumFactory;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class SchemalessIndexTest
{
  private static final Logger log = new Logger(SchemalessIndexTest.class);
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private static final String testFile = "druid.sample.json";
  private static final String TIMESTAMP = "timestamp";
  private static final List<String> METRICS = Arrays.asList("index");
  private static final AggregatorFactory[] METRIC_AGGS = new AggregatorFactory[]{
      new DoubleSumAggregatorFactory("index", "index"),
      new CountAggregatorFactory("count"),
      new HyperUniquesAggregatorFactory("quality_uniques", "quality")
  };
  private static final AggregatorFactory[] METRIC_AGGS_NO_UNIQ = new AggregatorFactory[]{
      new DoubleSumAggregatorFactory("index", "index"),
      new CountAggregatorFactory("count")
  };

  private static final IndexSpec indexSpec = new IndexSpec();

  private static final List<Map<String, Object>> events = Lists.newArrayList();

  private static final Map<Integer, Map<Integer, QueryableIndex>> incrementalIndexes = Maps.newHashMap();
  private static final Map<Integer, Map<Integer, QueryableIndex>> mergedIndexes = Maps.newHashMap();
  private static final List<QueryableIndex> rowPersistedIndexes = Lists.newArrayList();

  private static IncrementalIndex index = null;
  private static QueryableIndex mergedIndex = null;

  static {
    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(HyperLogLogHash.getDefault()));
    }
  }

  private final IndexMerger indexMerger;
  private final IndexIO indexIO;

  public SchemalessIndexTest(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
  {
    indexMerger = TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory);
    indexIO = TestHelper.getTestIndexIO(segmentWriteOutMediumFactory);
  }

  public static IncrementalIndex getIncrementalIndex()
  {
    synchronized (log) {
      if (index != null) {
        return index;
      }

      index = makeIncrementalIndex(testFile, METRIC_AGGS);

      return index;
    }
  }

  public static QueryableIndex getIncrementalIndex(int index1, int index2)
  {
    synchronized (log) {
      if (events.isEmpty()) {
        makeEvents();
      }

      Map<Integer, QueryableIndex> entry = incrementalIndexes.get(index1);
      if (entry != null) {
        QueryableIndex index = entry.get(index2);
        if (index != null) {
          return index;
        }
      } else {
        entry = Maps.<Integer, QueryableIndex>newHashMap();
        incrementalIndexes.put(index1, entry);
      }

      IncrementalIndex theIndex = null;

      int count = 0;
      for (final Map<String, Object> event : events) {
        if (count != index1 && count != index2) {
          count++;
          continue;
        }

        final long timestamp = new DateTime(event.get(TIMESTAMP), ISOChronology.getInstanceUTC()).getMillis();

        if (theIndex == null) {
          theIndex = new IncrementalIndex.Builder()
              .setIndexSchema(
                  new IncrementalIndexSchema.Builder()
                      .withMinTimestamp(timestamp)
                      .withQueryGranularity(Granularities.MINUTE)
                      .withMetrics(METRIC_AGGS)
                      .build()
              )
              .setMaxRowCount(1000)
              .buildOnheap();
        }

        final List<String> dims = Lists.newArrayList();
        for (final Map.Entry<String, Object> val : event.entrySet()) {
          if (!val.getKey().equalsIgnoreCase(TIMESTAMP) && !METRICS.contains(val.getKey())) {
            dims.add(val.getKey());
          }
        }

        try {
          theIndex.add(new MapBasedInputRow(timestamp, dims, event));
        }
        catch (IndexSizeExceededException e) {
          Throwables.propagate(e);
        }

        count++;
      }
      QueryableIndex retVal = TestIndex.persistRealtimeAndLoadMMapped(theIndex);
      entry.put(index2, retVal);
      return retVal;
    }
  }

  public QueryableIndex getMergedIncrementalIndex()
  {
    synchronized (log) {
      if (mergedIndex != null) {
        return mergedIndex;
      }

      try {
        IncrementalIndex top = makeIncrementalIndex("druid.sample.json.top", METRIC_AGGS);
        IncrementalIndex bottom = makeIncrementalIndex("druid.sample.json.bottom", METRIC_AGGS);

        File tmpFile = File.createTempFile("yay", "who");
        tmpFile.delete();

        File topFile = new File(tmpFile, "top");
        File bottomFile = new File(tmpFile, "bottom");
        File mergedFile = new File(tmpFile, "merged");

        topFile.mkdirs();
        topFile.deleteOnExit();
        bottomFile.mkdirs();
        bottomFile.deleteOnExit();
        mergedFile.mkdirs();
        mergedFile.deleteOnExit();

        indexMerger.persist(top, topFile, indexSpec, null);
        indexMerger.persist(bottom, bottomFile, indexSpec, null);

        mergedIndex = indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(indexIO.loadIndex(topFile), indexIO.loadIndex(bottomFile)),
                true,
                METRIC_AGGS,
                mergedFile,
                indexSpec,
                null
            )
        );

        return mergedIndex;
      }
      catch (IOException e) {
        mergedIndex = null;
        throw Throwables.propagate(e);
      }
    }
  }

  public QueryableIndex getMergedIncrementalIndex(int index1, int index2)
  {
    synchronized (log) {
      if (rowPersistedIndexes.isEmpty()) {
        makeRowPersistedIndexes();
      }

      Map<Integer, QueryableIndex> entry = mergedIndexes.get(index1);
      if (entry != null) {
        QueryableIndex index = entry.get(index2);
        if (index != null) {
          return index;
        }
      } else {
        entry = Maps.<Integer, QueryableIndex>newHashMap();
        mergedIndexes.put(index1, entry);
      }

      try {
        File tmpFile = File.createTempFile("yay", "who");
        tmpFile.delete();

        File mergedFile = new File(tmpFile, "merged");

        mergedFile.mkdirs();
        mergedFile.deleteOnExit();

        QueryableIndex index = indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(rowPersistedIndexes.get(index1), rowPersistedIndexes.get(index2)),
                true,
                METRIC_AGGS,
                mergedFile,
                indexSpec,
                null
            )
        );

        entry.put(index2, index);

        return index;
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  public QueryableIndex getMergedIncrementalIndex(int[] indexes)
  {
    synchronized (log) {
      if (rowPersistedIndexes.isEmpty()) {
        makeRowPersistedIndexes();
      }

      try {
        File tmpFile = File.createTempFile("yay", "who");
        tmpFile.delete();

        File mergedFile = new File(tmpFile, "merged");

        mergedFile.mkdirs();
        mergedFile.deleteOnExit();

        List<QueryableIndex> indexesToMerge = Lists.newArrayList();
        for (int index : indexes) {
          indexesToMerge.add(rowPersistedIndexes.get(index));
        }

        return indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(indexesToMerge, true, METRIC_AGGS, mergedFile, indexSpec, null)
        );
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  public QueryableIndex getAppendedIncrementalIndex(
      Iterable<Pair<String, AggregatorFactory[]>> files,
      List<Interval> intervals
  )
  {
    return makeAppendedMMappedIndex(files, intervals);
  }

  public QueryableIndex getMergedIncrementalIndexDiffMetrics()
  {
    return getMergedIncrementalIndex(
        Arrays.<Pair<String, AggregatorFactory[]>>asList(
            new Pair<String, AggregatorFactory[]>("druid.sample.json.top", METRIC_AGGS_NO_UNIQ),
            new Pair<String, AggregatorFactory[]>("druid.sample.json.bottom", METRIC_AGGS)
        )
    );
  }

  public QueryableIndex getMergedIncrementalIndex(Iterable<Pair<String, AggregatorFactory[]>> files)
  {
    return makeMergedMMappedIndex(files);
  }

  private static void makeEvents()
  {
    URL resource = TestIndex.class.getClassLoader().getResource(testFile);
    String filename = resource.getFile();
    log.info("Realtime loading index file[%s]", filename);
    try {
      for (Object obj : jsonMapper.readValue(new File(filename), List.class)) {
        final Map<String, Object> event = jsonMapper.convertValue(obj, Map.class);
        events.add(event);
      }
    }
    catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  private void makeRowPersistedIndexes()
  {
    synchronized (log) {
      try {
        if (events.isEmpty()) {
          makeEvents();
        }

        for (final Map<String, Object> event : events) {

          final long timestamp = new DateTime(event.get(TIMESTAMP), ISOChronology.getInstanceUTC()).getMillis();
          final List<String> dims = Lists.newArrayList();
          for (Map.Entry<String, Object> entry : event.entrySet()) {
            if (!entry.getKey().equalsIgnoreCase(TIMESTAMP) && !METRICS.contains(entry.getKey())) {
              dims.add(entry.getKey());
            }
          }

          final IncrementalIndex rowIndex = new IncrementalIndex.Builder()
              .setIndexSchema(
                  new IncrementalIndexSchema.Builder()
                      .withMinTimestamp(timestamp)
                      .withQueryGranularity(Granularities.MINUTE)
                      .withMetrics(METRIC_AGGS)
                      .build()
              )
              .setMaxRowCount(1000)
              .buildOnheap();

          rowIndex.add(
              new MapBasedInputRow(timestamp, dims, event)
          );

          File tmpFile = File.createTempFile("billy", "yay");
          tmpFile.delete();
          tmpFile.mkdirs();
          tmpFile.deleteOnExit();

          indexMerger.persist(rowIndex, tmpFile, indexSpec, null);
          rowPersistedIndexes.add(indexIO.loadIndex(tmpFile));
        }
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  public static IncrementalIndex makeIncrementalIndex(final String resourceFilename, AggregatorFactory[] aggs)
  {
    URL resource = TestIndex.class.getClassLoader().getResource(resourceFilename);
    log.info("Realtime loading resource[%s]", resource);
    String filename = resource.getFile();
    log.info("Realtime loading index file[%s]", filename);

    final IncrementalIndex retVal = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMinTimestamp(DateTimes.of("2011-01-12T00:00:00.000Z").getMillis())
                .withQueryGranularity(Granularities.MINUTE)
                .withMetrics(aggs)
                .build()
        )
        .setMaxRowCount(1000)
        .buildOnheap();

    try {
      final List<Object> events = jsonMapper.readValue(new File(filename), List.class);
      for (Object obj : events) {
        final Map<String, Object> event = jsonMapper.convertValue(obj, Map.class);

        final List<String> dims = Lists.newArrayList();
        for (Map.Entry<String, Object> entry : event.entrySet()) {
          if (!entry.getKey().equalsIgnoreCase(TIMESTAMP) && !METRICS.contains(entry.getKey())) {
            dims.add(entry.getKey());
          }
        }

        retVal.add(
            new MapBasedInputRow(
                new DateTime(event.get(TIMESTAMP), ISOChronology.getInstanceUTC()).getMillis(),
                dims,
                event
            )
        );
      }
    }
    catch (IOException e) {
      index = null;
      throw Throwables.propagate(e);
    }

    return retVal;
  }

  private List<File> makeFilesToMap(File tmpFile, Iterable<Pair<String, AggregatorFactory[]>> files) throws IOException
  {
    List<File> filesToMap = Lists.newArrayList();
    for (Pair<String, AggregatorFactory[]> file : files) {
      IncrementalIndex index = makeIncrementalIndex(file.lhs, file.rhs);
      File theFile = new File(tmpFile, file.lhs);
      theFile.mkdirs();
      theFile.deleteOnExit();
      filesToMap.add(theFile);
      indexMerger.persist(index, theFile, indexSpec, null);
    }

    return filesToMap;
  }

  private QueryableIndex makeAppendedMMappedIndex(
      Iterable<Pair<String, AggregatorFactory[]>> files,
      final List<Interval> intervals
  )
  {
    try {
      File tmpFile = File.createTempFile("yay", "boo");
      tmpFile.delete();
      File mergedFile = new File(tmpFile, "merged");
      mergedFile.mkdirs();
      mergedFile.deleteOnExit();

      List<File> filesToMap = makeFilesToMap(tmpFile, files);

      VersionedIntervalTimeline<Integer, File> timeline = new VersionedIntervalTimeline<Integer, File>(
          Comparators.naturalNullsFirst()
      );

      ShardSpec noneShardSpec = NoneShardSpec.instance();

      for (int i = 0; i < intervals.size(); i++) {
        timeline.add(intervals.get(i), i, noneShardSpec.createChunk(filesToMap.get(i)));
      }

      final List<IndexableAdapter> adapters = Lists.newArrayList(
          Iterables.concat(
              // TimelineObjectHolder is actually an iterable of iterable of indexable adapters
              Iterables.transform(
                  timeline.lookup(Intervals.of("1000-01-01/3000-01-01")),
                  new Function<TimelineObjectHolder<Integer, File>, Iterable<IndexableAdapter>>()
                  {
                    @Override
                    public Iterable<IndexableAdapter> apply(final TimelineObjectHolder<Integer, File> timelineObjectHolder)
                    {
                      return Iterables.transform(
                          timelineObjectHolder.getObject(),

                          // Each chunk can be used to build the actual IndexableAdapter
                          new Function<PartitionChunk<File>, IndexableAdapter>()
                          {
                            @Override
                            public IndexableAdapter apply(PartitionChunk<File> chunk)
                            {
                              try {
                                return new RowFilteringIndexAdapter(
                                    new QueryableIndexIndexableAdapter(indexIO.loadIndex(chunk.getObject())),
                                    rowPointer -> timelineObjectHolder.getInterval().contains(rowPointer.getTimestamp())
                                );
                              }
                              catch (IOException e) {
                                throw Throwables.propagate(e);
                              }
                            }
                          }
                      );
                    }
                  }
              )
          )
      );

      return indexIO.loadIndex(indexMerger.append(adapters, null, mergedFile, indexSpec, null));
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private QueryableIndex makeMergedMMappedIndex(Iterable<Pair<String, AggregatorFactory[]>> files)
  {
    try {
      File tmpFile = File.createTempFile("yay", "who");
      tmpFile.delete();
      File mergedFile = new File(tmpFile, "merged");
      mergedFile.mkdirs();
      mergedFile.deleteOnExit();

      List<File> filesToMap = makeFilesToMap(tmpFile, files);

      return indexIO.loadIndex(
          indexMerger.mergeQueryableIndex(
              Lists.newArrayList(
                  Iterables.transform(
                      filesToMap,
                      new Function<File, QueryableIndex>()
                      {
                        @Override
                        public QueryableIndex apply(@Nullable File input)
                        {
                          try {
                            return indexIO.loadIndex(input);
                          }
                          catch (IOException e) {
                            throw Throwables.propagate(e);
                          }
                        }
                      }
                  )
              ),
              true,
              METRIC_AGGS,
              mergedFile,
              indexSpec,
              null
          )
      );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
