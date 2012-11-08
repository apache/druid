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

package com.metamx.druid.index.v1;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.google.common.io.LineProcessor;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.DoubleSumAggregatorFactory;
import com.metamx.druid.client.RangeIterable;
import com.metamx.druid.guava.GuavaUtils;
import com.metamx.druid.index.v1.serde.ComplexMetricSerde;
import com.metamx.druid.index.v1.serde.ComplexMetrics;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.kv.ArrayIndexed;
import com.metamx.druid.kv.Indexed;
import com.metamx.druid.kv.IndexedFloats;
import com.metamx.druid.kv.IndexedInts;
import com.metamx.druid.kv.IndexedLongs;
import com.metamx.druid.kv.Indexedids;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class TestIndex
{
  private static final Logger log = new Logger(TestIndex.class);

  private static Index index = null;
  private static Index unionIndexTop = null;
  private static Index unionIndexBottom = null;
  private static IncrementalIndex realtimeIndex = null;
  private static MMappedIndex mmappedIndex = null;
  private static MMappedIndex mergedRealtime = null;

  public static final String[] DIMENSIONS = new String[]{"provider", "quality", "placement", "placementish"};
  public static final String[] METRICS = new String[]{"index"};
  public static final Map<String, Integer> dimIds = Maps.uniqueIndex(
      new RangeIterable(4),
      new Function<Integer, String>()
      {
        @Override
        public String apply(@Nullable Integer input)
        {
          return DIMENSIONS[input];
        }
      }
  );
  private static final Interval DATA_INTERVAL = new Interval("2011-01-12T00:00:00.000Z/2011-04-16T00:00:00.000Z");
  private static final AggregatorFactory[] METRIC_AGGS = new AggregatorFactory[]{
      new DoubleSumAggregatorFactory(METRICS[0], METRICS[0])
  };

  public static Index convertMMapToIndex(MMappedIndex mmappedIndex)
  {
    Indexed<String> dimsIndexed = mmappedIndex.getAvailableDimensions();
    String[] dimensions = new String[dimsIndexed.size()];
    for (int i = 0; i < dimsIndexed.size(); ++i) {
      dimensions[i] = dimsIndexed.get(i);
    }

    Indexed<String> metricsIndexed = mmappedIndex.getAvailableMetrics();
    String[] metrics = new String[metricsIndexed.size()];
    for (int i = 0; i < metricsIndexed.size(); ++i) {
      metrics[i] = metricsIndexed.get(i);
    }

    IndexedLongs timeBuf = mmappedIndex.getReadOnlyTimestamps();
    long[] timestamps = new long[timeBuf.size()];
    timeBuf.fill(0, timestamps);
    Closeables.closeQuietly(timeBuf);

    Map<String, MetricHolder> metricVals = Maps.newLinkedHashMap();
    for (String metric : metrics) {
      MetricHolder holder = mmappedIndex.getMetricHolder(metric);
      switch (holder.getType()) {
        case FLOAT:
          IndexedFloats mmappedFloats = holder.getFloatType();
          float[] metricValsArray = new float[mmappedFloats.size()];
          mmappedFloats.fill(0, metricValsArray);
          Closeables.closeQuietly(mmappedFloats);

          metricVals.put(
              metric,
              MetricHolder.floatMetric(
                  metric,
                  CompressedFloatsIndexedSupplier.fromFloatBuffer(
                      FloatBuffer.wrap(metricValsArray),
                      ByteOrder.nativeOrder()
                  )
              )
          );
          break;
        case COMPLEX:
          Indexed complexObjects = holder.getComplexType();
          Object[] vals = new Object[complexObjects.size()];
          for (int i = 0; i < complexObjects.size(); ++i) {
            vals[i] = complexObjects.get(i);
          }

          final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(holder.getTypeName());
          if (serde == null) {
            throw new ISE("Unknown type[%s]", holder.getTypeName());
          }

          metricVals.put(
              metric,
              MetricHolder.complexMetric(
                  metric,
                  holder.getTypeName(),
                  new ArrayIndexed(vals, serde.getObjectStrategy().getClazz())
              )
          );
          break;
      }
    }

    Map<String, Map<String, Integer>> dimIdLookup = Maps.newHashMap();
    Map<String, String[]> reverseDimLookup = Maps.newHashMap();
    Map<String, ImmutableConciseSet[]> invertedIndexesMap = Maps.newHashMap();
    Map<String, DimensionColumn> dimensionColumns = Maps.newHashMap();

    for (String dimension : dimensions) {
      final Indexed<String> dimValueLookup = mmappedIndex.getDimValueLookup(dimension);
      String[] values = new String[dimValueLookup.size()];
      for (int i = 0; i < dimValueLookup.size(); ++i) {
        values[i] = dimValueLookup.get(i);
      }

      Map<String, Integer> lookupMap = Maps.newHashMapWithExpectedSize(dimValueLookup.size());
      for (int i = 0; i < values.length; i++) {
        lookupMap.put(values[i], i);
      }

      ImmutableConciseSet[] invertedIndexes = new ImmutableConciseSet[values.length];
      final Indexed<String> dimValuesIndexed = mmappedIndex.getDimValueLookup(dimension);
      for (int i = 0; i < dimValuesIndexed.size(); ++i) {
        invertedIndexes[i] = mmappedIndex.getInvertedIndex(dimension, dimValuesIndexed.get(i));
      }

      int[] dimValues = new int[timestamps.length];
      Map<List<Integer>, Integer> rowGroupings = Maps.newHashMap();
      final Indexed<? extends IndexedInts> dimColumn = mmappedIndex.getDimColumn(dimension);
      for (int i = 0; i < dimColumn.size(); ++i) {
        int[] expansionValue = Indexedids.arrayFromIndexedInts(dimColumn.get(i));
        Integer value = rowGroupings.get(Ints.asList(expansionValue));
        if (value == null) {
          value = rowGroupings.size();
          rowGroupings.put(Ints.asList(expansionValue), value);
        }
        dimValues[i] = value;
      }

      int[][] expansionValues = new int[rowGroupings.size()][];
      for (Map.Entry<List<Integer>, Integer> entry : rowGroupings.entrySet()) {
        expansionValues[entry.getValue()] = Ints.toArray(entry.getKey());
      }

      dimIdLookup.put(dimension, lookupMap);
      reverseDimLookup.put(dimension, values);
      invertedIndexesMap.put(dimension, invertedIndexes);
      dimensionColumns.put(dimension, new DimensionColumn(expansionValues, dimValues));
    }

    return new Index(
        dimensions,
        metrics,
        mmappedIndex.getDataInterval(),
        timestamps,
        metricVals,
        dimIdLookup,
        reverseDimLookup,
        invertedIndexesMap,
        dimensionColumns
    );
  }

  public static Index getTestIndex() throws IOException
  {
    synchronized (log) {
      if (index != null) {
        return index;
      }
    }

    return index = convertMMapToIndex(getMMappedTestIndex());
  }

  public static Index getTestUnionIndexTop() throws IOException
  {
    synchronized (log) {
      if (unionIndexTop != null) {
        return unionIndexTop;
      }
    }

    IncrementalIndex incrementalIndex = makeRealtimeIndex("druid.sample.tsv.top");
    MMappedIndex mmapped = persistRealtimeAndLoadMMapped(incrementalIndex);

    return unionIndexTop = convertMMapToIndex(mmapped);
  }

  public static Index getTestUnionIndexBottom() throws IOException
  {
    synchronized (log) {
      if (unionIndexBottom != null) {
        return unionIndexBottom;
      }
    }

    IncrementalIndex incrementalIndex = makeRealtimeIndex("druid.sample.tsv.bottom");
    MMappedIndex mmapped = persistRealtimeAndLoadMMapped(incrementalIndex);

    return unionIndexBottom = convertMMapToIndex(mmapped);
  }

  public static IncrementalIndex getIncrementalTestIndex()
  {
    synchronized (log) {
      if (realtimeIndex != null) {
        return realtimeIndex;
      }
    }

    return realtimeIndex = makeRealtimeIndex("druid.sample.tsv");
  }

  public static MMappedIndex getMMappedTestIndex()
  {
    synchronized (log) {
      if (mmappedIndex != null) {
        return mmappedIndex;
      }
    }

    IncrementalIndex incrementalIndex = getIncrementalTestIndex();
    mmappedIndex = persistRealtimeAndLoadMMapped(incrementalIndex);

    return mmappedIndex;
  }

  public static MMappedIndex mergedRealtimeIndex()
  {
    synchronized (log) {
      if (mergedRealtime != null) {
        return mergedRealtime;
      }

      try {
        IncrementalIndex top = makeRealtimeIndex("druid.sample.tsv.top");
        IncrementalIndex bottom = makeRealtimeIndex("druid.sample.tsv.bottom");

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

        IndexMerger.persist(top, DATA_INTERVAL, topFile);
        IndexMerger.persist(bottom, DATA_INTERVAL, bottomFile);

        mergedRealtime = com.metamx.druid.index.v1.IndexIO.mapDir(
            IndexMerger.mergeMMapped(
                Arrays.asList(
                    com.metamx.druid.index.v1.IndexIO.mapDir(topFile),
                    com.metamx.druid.index.v1.IndexIO.mapDir(bottomFile)
                ), METRIC_AGGS, mergedFile
            )
        );

        return mergedRealtime;
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private static IncrementalIndex makeRealtimeIndex(final String resourceFilename)
  {
    URL resource = TestIndex.class.getClassLoader().getResource(resourceFilename);
    String filename = resource.getFile();
    log.info("Realtime loading index file[%s]", filename);

    final IncrementalIndex retVal = new IncrementalIndex(
        new DateTime("2011-01-12T00:00:00.000Z").getMillis(), QueryGranularity.NONE, METRIC_AGGS
    );

    final AtomicLong startTime = new AtomicLong();
    int lineCount;
    try {
      lineCount = CharStreams.readLines(
          GuavaUtils.joinFiles(new File(filename)),
          new LineProcessor<Integer>()
          {
            boolean runOnce = false;
            int lineCount = 0;

            @Override
            public boolean processLine(String line) throws IOException
            {
              if (!runOnce) {
                startTime.set(System.currentTimeMillis());
                runOnce = true;
              }

              final String[] splits = line.split("\t");

              retVal.add(
                  new InputRow()
                  {
                    @Override
                    public long getTimestampFromEpoch()
                    {
                      return new DateTime(splits[0]).getMillis();
                    }

                    @Override
                    public List<String> getDimensions()
                    {
                      return Arrays.asList(DIMENSIONS);
                    }

                    @Override
                    public List<String> getDimension(String dimension)
                    {
                      return Arrays.asList(splits[dimIds.get(dimension) + 1].split("\u0001"));
                    }

                    @Override
                    public float getFloatMetric(String metric)
                    {
                      Preconditions.checkArgument(METRICS[0].equals(metric), "WTF!?");
                      return Float.parseFloat(splits[5]);
                    }
                  }
              );

              ++lineCount;
              return true;
            }

            @Override
            public Integer getResult()
            {
              return lineCount;
            }
          }
      );
    }
    catch (IOException e) {
      realtimeIndex = null;
      throw Throwables.propagate(e);
    }

    log.info("Loaded %,d lines in %,d millis.", lineCount, System.currentTimeMillis() - startTime.get());

    return retVal;
  }

  public static MMappedIndex persistRealtimeAndLoadMMapped(IncrementalIndex index)
  {
    try {
      File someTmpFile = File.createTempFile("billy", "yay");
      someTmpFile.delete();
      someTmpFile.mkdirs();
      someTmpFile.deleteOnExit();

      IndexMerger.persist(index, someTmpFile);
      return com.metamx.druid.index.v1.IndexIO.mapDir(someTmpFile);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
