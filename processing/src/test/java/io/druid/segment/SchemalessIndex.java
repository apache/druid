/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.hash.Hashing;
import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class SchemalessIndex
{
  private static final Logger log = new Logger(SchemalessIndex.class);
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

  private static final List<Map<String, Object>> events = Lists.newArrayList();

  private static final Map<Integer, Map<Integer, QueryableIndex>> incrementalIndexes = Maps.newHashMap();
  private static final Map<Integer, Map<Integer, QueryableIndex>> mergedIndexes = Maps.newHashMap();
  private static final List<QueryableIndex> rowPersistedIndexes = Lists.newArrayList();

  private static IncrementalIndex index = null;
  private static QueryableIndex mergedIndex = null;

  static {
    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(Hashing.murmur3_128()));
    }
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

        final long timestamp = new DateTime(event.get(TIMESTAMP)).getMillis();

        if (theIndex == null) {
          theIndex = new IncrementalIndex(timestamp, QueryGranularity.MINUTE, METRIC_AGGS);
        }

        final List<String> dims = Lists.newArrayList();
        for (final Map.Entry<String, Object> val : event.entrySet()) {
          if (!val.getKey().equalsIgnoreCase(TIMESTAMP) && !METRICS.contains(val.getKey())) {
            dims.add(val.getKey());
          }
        }

        theIndex.add(new MapBasedInputRow(timestamp, dims, event));

        count++;
      }
      QueryableIndex retVal = TestIndex.persistRealtimeAndLoadMMapped(theIndex);
      entry.put(index2, retVal);
      return retVal;
    }
  }

  public static QueryableIndex getMergedIncrementalIndex()
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

        IndexMerger.persist(top, topFile);
        IndexMerger.persist(bottom, bottomFile);

        mergedIndex = io.druid.segment.IndexIO.loadIndex(
            IndexMerger.mergeQueryableIndex(
                Arrays.asList(IndexIO.loadIndex(topFile), IndexIO.loadIndex(bottomFile)), METRIC_AGGS, mergedFile
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

  public static QueryableIndex getMergedIncrementalIndex(int index1, int index2)
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

        QueryableIndex index = IndexIO.loadIndex(
            IndexMerger.mergeQueryableIndex(
                Arrays.asList(rowPersistedIndexes.get(index1), rowPersistedIndexes.get(index2)), METRIC_AGGS, mergedFile
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

  public static QueryableIndex getMergedIncrementalIndex(int[] indexes)
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
        for (int i = 0; i < indexes.length; i++) {
          indexesToMerge.add(rowPersistedIndexes.get(indexes[i]));
        }

        QueryableIndex index = IndexIO.loadIndex(
            IndexMerger.mergeQueryableIndex(indexesToMerge, METRIC_AGGS, mergedFile)
        );

        return index;
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  public static QueryableIndex getAppendedIncrementalIndex(
      Iterable<Pair<String, AggregatorFactory[]>> files,
      List<Interval> intervals
  )
  {
    return makeAppendedMMappedIndex(files, intervals);
  }

  public static QueryableIndex getMergedIncrementalIndexDiffMetrics()
  {
    return getMergedIncrementalIndex(
        Arrays.<Pair<String, AggregatorFactory[]>>asList(
            new Pair<String, AggregatorFactory[]>("druid.sample.json.top", METRIC_AGGS_NO_UNIQ),
            new Pair<String, AggregatorFactory[]>("druid.sample.json.bottom", METRIC_AGGS)
        )
    );
  }

  public static QueryableIndex getMergedIncrementalIndex(Iterable<Pair<String, AggregatorFactory[]>> files)
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

  private static void makeRowPersistedIndexes()
  {
    synchronized (log) {
      try {
        if (events.isEmpty()) {
          makeEvents();
        }

        for (final Map<String, Object> event : events) {

          final long timestamp = new DateTime(event.get(TIMESTAMP)).getMillis();
          final List<String> dims = Lists.newArrayList();
          for (Map.Entry<String, Object> entry : event.entrySet()) {
            if (!entry.getKey().equalsIgnoreCase(TIMESTAMP) && !METRICS.contains(entry.getKey())) {
              dims.add(entry.getKey());
            }
          }

          final IncrementalIndex rowIndex = new IncrementalIndex(
              timestamp, QueryGranularity.MINUTE, METRIC_AGGS
          );

          rowIndex.add(
              new MapBasedInputRow(timestamp, dims, event)
          );

          File tmpFile = File.createTempFile("billy", "yay");
          tmpFile.delete();
          tmpFile.mkdirs();
          tmpFile.deleteOnExit();

          IndexMerger.persist(rowIndex, tmpFile);
          rowPersistedIndexes.add(IndexIO.loadIndex(tmpFile));
        }
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private static IncrementalIndex makeIncrementalIndex(final String resourceFilename, AggregatorFactory[] aggs)
  {
    URL resource = TestIndex.class.getClassLoader().getResource(resourceFilename);
    log.info("Realtime loading resource[%s]", resource);
    String filename = resource.getFile();
    log.info("Realtime loading index file[%s]", filename);

    final IncrementalIndex retVal = new IncrementalIndex(
        new DateTime("2011-01-12T00:00:00.000Z").getMillis(), QueryGranularity.MINUTE, aggs
    );

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
                new DateTime(event.get(TIMESTAMP)).getMillis(),
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

  private static List<File> makeFilesToMap(File tmpFile, Iterable<Pair<String, AggregatorFactory[]>> files)
      throws IOException
  {
    List<File> filesToMap = Lists.newArrayList();
    for (Pair<String, AggregatorFactory[]> file : files) {
      IncrementalIndex index = makeIncrementalIndex(file.lhs, file.rhs);
      File theFile = new File(tmpFile, file.lhs);
      theFile.mkdirs();
      theFile.deleteOnExit();
      filesToMap.add(theFile);
      IndexMerger.persist(index, theFile);
    }

    return filesToMap;
  }

  private static QueryableIndex makeAppendedMMappedIndex(
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

      List<IndexableAdapter> adapters = Lists.newArrayList();

      VersionedIntervalTimeline<Integer, File> timeline = new VersionedIntervalTimeline<Integer, File>(
          Ordering.natural().nullsFirst()
      );

      ShardSpec noneShardSpec = new NoneShardSpec();

      for (int i = 0; i < intervals.size(); i++) {
        timeline.add(intervals.get(i), i, noneShardSpec.createChunk(filesToMap.get(i)));
      }

      List<Pair<File, Interval>> intervalsToMerge = Lists.transform(
          timeline.lookup(new Interval("1000-01-01/3000-01-01")),
          new Function<TimelineObjectHolder<Integer, File>, Pair<File, Interval>>()
          {
            @Override
            public Pair<File, Interval> apply(@Nullable TimelineObjectHolder<Integer, File> input)
            {
              return new Pair<File, Interval>(input.getObject().getChunk(0).getObject(), input.getInterval());
            }
          }
      );

      for (final Pair<File, Interval> pair : intervalsToMerge) {
        adapters.add(
            new RowboatFilteringIndexAdapter(
                new QueryableIndexIndexableAdapter(IndexIO.loadIndex(pair.lhs)),
                new Predicate<Rowboat>()
                {
                  @Override
                  public boolean apply(@Nullable Rowboat input)
                  {
                    return pair.rhs.contains(input.getTimestamp());
                  }
                }
            )
        );
      }

      return IndexIO.loadIndex(IndexMerger.append(adapters, mergedFile));
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private static QueryableIndex makeMergedMMappedIndex(Iterable<Pair<String, AggregatorFactory[]>> files)
  {
    try {
      File tmpFile = File.createTempFile("yay", "who");
      tmpFile.delete();
      File mergedFile = new File(tmpFile, "merged");
      mergedFile.mkdirs();
      mergedFile.deleteOnExit();

      List<File> filesToMap = makeFilesToMap(tmpFile, files);

      return IndexIO.loadIndex(
          IndexMerger.mergeQueryableIndex(
              Lists.newArrayList(
                  Iterables.transform(
                      filesToMap,
                      new Function<File, QueryableIndex>()
                      {
                        @Override
                        public QueryableIndex apply(@Nullable File input)
                        {
                          try {
                            return IndexIO.loadIndex(input);
                          }
                          catch (IOException e) {
                            throw Throwables.propagate(e);
                          }
                        }
                      }
                  )
              ),
              METRIC_AGGS,
              mergedFile
          )
      );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
