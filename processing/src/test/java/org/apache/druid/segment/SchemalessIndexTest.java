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

package org.apache.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.timeline.Overshadowable;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class SchemalessIndexTest
{
  private static final Logger log = new Logger(SchemalessIndexTest.class);
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  private static final String TEST_FILE = "druid.sample.json";
  private static final String TIMESTAMP = "timestamp";
  private static final List<String> METRICS = Collections.singletonList("index");
  private static final AggregatorFactory[] METRIC_AGGS = new AggregatorFactory[]{
      new DoubleSumAggregatorFactory("index", "index"),
      new CountAggregatorFactory("count"),
      new HyperUniquesAggregatorFactory("quality_uniques", "quality")
  };
  private static final AggregatorFactory[] METRIC_AGGS_NO_UNIQ = new AggregatorFactory[]{
      new DoubleSumAggregatorFactory("index", "index"),
      new CountAggregatorFactory("count")
  };

  private static final IndexSpec INDEX_SPEC = new IndexSpec();

  private static final List<Map<String, Object>> EVENTS = new ArrayList<>();

  private static final Map<Integer, Map<Integer, QueryableIndex>> INCREMENTAL_INDEXES = new HashMap<>();
  private static final Map<Integer, Map<Integer, QueryableIndex>> MERGED_INDEXES = new HashMap<>();
  private static final List<QueryableIndex> ROW_PERSISTED_INDEXES = new ArrayList<>();

  private static IncrementalIndex index = null;
  private static QueryableIndex mergedIndex = null;

  static {
    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde());
  }

  private final IndexMerger indexMerger;
  private final IndexIO indexIO;

  public SchemalessIndexTest(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
  {
    indexMerger = TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory);
    indexIO = TestHelper.getTestIndexIO();
  }

  public static IncrementalIndex getIncrementalIndex()
  {
    synchronized (log) {
      if (index != null) {
        return index;
      }

      index = makeIncrementalIndex(TEST_FILE, METRIC_AGGS);

      return index;
    }
  }

  public static QueryableIndex getIncrementalIndex(int index1, int index2)
  {
    synchronized (log) {
      if (EVENTS.isEmpty()) {
        makeEvents();
      }

      Map<Integer, QueryableIndex> entry = INCREMENTAL_INDEXES.get(index1);
      if (entry != null) {
        QueryableIndex index = entry.get(index2);
        if (index != null) {
          return index;
        }
      } else {
        entry = new HashMap<>();
        INCREMENTAL_INDEXES.put(index1, entry);
      }

      IncrementalIndex theIndex = null;

      int count = 0;
      for (final Map<String, Object> event : EVENTS) {
        if (count != index1 && count != index2) {
          count++;
          continue;
        }

        final long timestamp = new DateTime(event.get(TIMESTAMP), ISOChronology.getInstanceUTC()).getMillis();

        if (theIndex == null) {
          theIndex = new OnheapIncrementalIndex.Builder()
              .setIndexSchema(
                  new IncrementalIndexSchema.Builder()
                      .withMinTimestamp(timestamp)
                      .withQueryGranularity(Granularities.MINUTE)
                      .withMetrics(METRIC_AGGS)
                      .build()
              )
              .setMaxRowCount(1000)
              .build();
        }

        final List<String> dims = new ArrayList<>();
        for (final Map.Entry<String, Object> val : event.entrySet()) {
          if (!val.getKey().equalsIgnoreCase(TIMESTAMP) && !METRICS.contains(val.getKey())) {
            dims.add(val.getKey());
          }
        }

        try {
          theIndex.add(new MapBasedInputRow(timestamp, dims, event));
        }
        catch (IndexSizeExceededException e) {
          throw new RuntimeException(e);
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

        indexMerger.persist(top, topFile, INDEX_SPEC, null);
        indexMerger.persist(bottom, bottomFile, INDEX_SPEC, null);

        mergedIndex = indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(indexIO.loadIndex(topFile), indexIO.loadIndex(bottomFile)),
                true,
                METRIC_AGGS,
                mergedFile,
                INDEX_SPEC,
                null,
                -1
            )
        );

        return mergedIndex;
      }
      catch (IOException e) {
        mergedIndex = null;
        throw new RuntimeException(e);
      }
    }
  }

  public QueryableIndex getMergedIncrementalIndex(int index1, int index2)
  {
    synchronized (log) {
      if (ROW_PERSISTED_INDEXES.isEmpty()) {
        makeRowPersistedIndexes();
      }

      Map<Integer, QueryableIndex> entry = MERGED_INDEXES.get(index1);
      if (entry != null) {
        QueryableIndex index = entry.get(index2);
        if (index != null) {
          return index;
        }
      } else {
        entry = new HashMap<>();
        MERGED_INDEXES.put(index1, entry);
      }

      try {
        File tmpFile = File.createTempFile("yay", "who");
        tmpFile.delete();

        File mergedFile = new File(tmpFile, "merged");

        mergedFile.mkdirs();
        mergedFile.deleteOnExit();

        QueryableIndex index = indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(ROW_PERSISTED_INDEXES.get(index1), ROW_PERSISTED_INDEXES.get(index2)),
                true,
                METRIC_AGGS,
                mergedFile,
                INDEX_SPEC,
                null,
                -1
            )
        );

        entry.put(index2, index);

        return index;
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public QueryableIndex getMergedIncrementalIndex(int[] indexes)
  {
    synchronized (log) {
      if (ROW_PERSISTED_INDEXES.isEmpty()) {
        makeRowPersistedIndexes();
      }

      try {
        File tmpFile = File.createTempFile("yay", "who");
        tmpFile.delete();

        File mergedFile = new File(tmpFile, "merged");

        mergedFile.mkdirs();
        mergedFile.deleteOnExit();

        List<QueryableIndex> indexesToMerge = new ArrayList<>();
        for (int index : indexes) {
          indexesToMerge.add(ROW_PERSISTED_INDEXES.get(index));
        }

        return indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(indexesToMerge, true, METRIC_AGGS, mergedFile, INDEX_SPEC, null, -1)
        );
      }
      catch (IOException e) {
        throw new RuntimeException(e);
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
        Arrays.asList(
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
    URL resource = TestIndex.class.getClassLoader().getResource(TEST_FILE);
    String filename = resource.getFile();
    log.info("Realtime loading index file[%s]", filename);
    try {
      for (Object obj : JSON_MAPPER.readValue(new File(filename), List.class)) {
        final Map<String, Object> event = JSON_MAPPER.convertValue(obj, Map.class);
        EVENTS.add(event);
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void makeRowPersistedIndexes()
  {
    synchronized (log) {
      try {
        if (EVENTS.isEmpty()) {
          makeEvents();
        }

        for (final Map<String, Object> event : EVENTS) {

          final long timestamp = new DateTime(event.get(TIMESTAMP), ISOChronology.getInstanceUTC()).getMillis();
          final List<String> dims = new ArrayList<>();
          for (Map.Entry<String, Object> entry : event.entrySet()) {
            if (!entry.getKey().equalsIgnoreCase(TIMESTAMP) && !METRICS.contains(entry.getKey())) {
              dims.add(entry.getKey());
            }
          }

          final IncrementalIndex rowIndex = new OnheapIncrementalIndex.Builder()
              .setIndexSchema(
                  new IncrementalIndexSchema.Builder()
                      .withMinTimestamp(timestamp)
                      .withQueryGranularity(Granularities.MINUTE)
                      .withMetrics(METRIC_AGGS)
                      .build()
              )
              .setMaxRowCount(1000)
              .build();

          rowIndex.add(
              new MapBasedInputRow(timestamp, dims, event)
          );

          File tmpFile = File.createTempFile("billy", "yay");
          tmpFile.delete();
          tmpFile.mkdirs();
          tmpFile.deleteOnExit();

          indexMerger.persist(rowIndex, tmpFile, INDEX_SPEC, null);
          ROW_PERSISTED_INDEXES.add(indexIO.loadIndex(tmpFile));
        }
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static IncrementalIndex makeIncrementalIndex(final String resourceFilename, AggregatorFactory[] aggs)
  {
    URL resource = TestIndex.class.getClassLoader().getResource(resourceFilename);
    log.info("Realtime loading resource[%s]", resource);
    String filename = resource.getFile();
    log.info("Realtime loading index file[%s]", filename);

    final IncrementalIndex retVal = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMinTimestamp(DateTimes.of("2011-01-12T00:00:00.000Z").getMillis())
                .withQueryGranularity(Granularities.MINUTE)
                .withMetrics(aggs)
                .build()
        )
        .setMaxRowCount(1000)
        .build();

    try {
      final List<Object> events = JSON_MAPPER.readValue(new File(filename), List.class);
      for (Object obj : events) {
        final Map<String, Object> event = JSON_MAPPER.convertValue(obj, Map.class);

        final List<String> dims = new ArrayList<>();
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
      throw new RuntimeException(e);
    }

    return retVal;
  }

  private List<File> makeFilesToMap(File tmpFile, Iterable<Pair<String, AggregatorFactory[]>> files) throws IOException
  {
    List<File> filesToMap = new ArrayList<>();
    for (Pair<String, AggregatorFactory[]> file : files) {
      IncrementalIndex index = makeIncrementalIndex(file.lhs, file.rhs);
      File theFile = new File(tmpFile, file.lhs);
      theFile.mkdirs();
      theFile.deleteOnExit();
      filesToMap.add(theFile);
      indexMerger.persist(index, theFile, INDEX_SPEC, null);
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

      VersionedIntervalTimeline<Integer, OvershadowableFile> timeline = new VersionedIntervalTimeline<>(
          Comparators.naturalNullsFirst()
      );

      ShardSpec noneShardSpec = NoneShardSpec.instance();

      for (int i = 0; i < intervals.size(); i++) {
        timeline.add(intervals.get(i), i, noneShardSpec.createChunk(new OvershadowableFile(i, filesToMap.get(i))));
      }

      final List<IndexableAdapter> adapters = Lists.newArrayList(
          Iterables.concat(
              // TimelineObjectHolder is actually an iterable of iterable of indexable adapters
              Iterables.transform(
                  timeline.lookup(Intervals.of("1000-01-01/3000-01-01")),
                  new Function<TimelineObjectHolder<Integer, OvershadowableFile>, Iterable<IndexableAdapter>>()
                  {
                    @Override
                    public Iterable<IndexableAdapter> apply(final TimelineObjectHolder<Integer, OvershadowableFile> timelineObjectHolder)
                    {
                      return Iterables.transform(
                          timelineObjectHolder.getObject(),

                          // Each chunk can be used to build the actual IndexableAdapter
                          new Function<PartitionChunk<OvershadowableFile>, IndexableAdapter>()
                          {
                            @Override
                            public IndexableAdapter apply(PartitionChunk<OvershadowableFile> chunk)
                            {
                              try {
                                return new RowFilteringIndexAdapter(
                                    new QueryableIndexIndexableAdapter(indexIO.loadIndex(chunk.getObject().file)),
                                    rowPointer -> timelineObjectHolder.getInterval().contains(rowPointer.getTimestamp())
                                );
                              }
                              catch (IOException e) {
                                throw new RuntimeException(e);
                              }
                            }
                          }
                      );
                    }
                  }
              )
          )
      );

      return indexIO.loadIndex(indexMerger.append(adapters, null, mergedFile, INDEX_SPEC, null));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
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
                            throw new RuntimeException(e);
                          }
                        }
                      }
                  )
              ),
              true,
              METRIC_AGGS,
              mergedFile,
              INDEX_SPEC,
              null,
              -1
          )
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static class OvershadowableFile implements Overshadowable<OvershadowableFile>
  {
    private final String majorVersion;
    private final File file;

    OvershadowableFile(int majorVersion, File file)
    {
      this.majorVersion = Integer.toString(majorVersion);
      this.file = file;
    }

    @Override
    public boolean overshadows(OvershadowableFile other)
    {
      return false;
    }

    @Override
    public int getStartRootPartitionId()
    {
      return 0;
    }

    @Override
    public int getEndRootPartitionId()
    {
      return 0;
    }

    @Override
    public String getVersion()
    {
      return majorVersion;
    }

    @Override
    public short getMinorVersion()
    {
      return 0;
    }

    @Override
    public short getAtomicUpdateGroupSize()
    {
      return 0;
    }
  }
}
