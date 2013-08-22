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
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.Ints;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.collections.spatial.RTree;
import com.metamx.collections.spatial.split.LinearGutmanSplitStrategy;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.MergeIterable;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.common.io.smoosh.Smoosh;
import com.metamx.common.logger.Logger;
import com.metamx.druid.CombiningIterable;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.ToLowerCaseAggregatorFactory;
import com.metamx.druid.guava.FileOutputSupplier;
import com.metamx.druid.guava.GuavaUtils;
import com.metamx.druid.index.QueryableIndex;
import com.metamx.druid.index.v1.serde.ComplexMetricSerde;
import com.metamx.druid.index.v1.serde.ComplexMetrics;
import com.metamx.druid.kv.ByteBufferWriter;
import com.metamx.druid.kv.ConciseCompressedIndexedInts;
import com.metamx.druid.kv.GenericIndexed;
import com.metamx.druid.kv.GenericIndexedWriter;
import com.metamx.druid.kv.IOPeon;
import com.metamx.druid.kv.Indexed;
import com.metamx.druid.kv.IndexedInts;
import com.metamx.druid.kv.IndexedIterable;
import com.metamx.druid.kv.IndexedRTree;
import com.metamx.druid.kv.TmpFileIOPeon;
import com.metamx.druid.kv.VSizeIndexedWriter;
import com.metamx.druid.utils.JodaUtils;
import com.metamx.druid.utils.SerializerUtils;
import it.uniroma3.mat.extendedset.intset.ConciseSet;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 */
public class IndexMerger
{
  private static final Logger log = new Logger(IndexMerger.class);

  private static final SerializerUtils serializerUtils = new SerializerUtils();
  private static final int INVALID_ROW = -1;
  private static final Splitter SPLITTER = Splitter.on(",");

  public static File persist(final IncrementalIndex index, File outDir) throws IOException
  {
    return persist(index, index.getInterval(), outDir);
  }

  /**
   * This is *not* thread-safe and havok will ensue if this is called and writes are still occurring
   * on the IncrementalIndex object.
   *
   * @param index        the IncrementalIndex to persist
   * @param dataInterval the Interval that the data represents
   * @param outDir       the directory to persist the data to
   *
   * @throws java.io.IOException
   */
  public static File persist(final IncrementalIndex index, final Interval dataInterval, File outDir) throws IOException
  {
    return persist(index, dataInterval, outDir, new NoopProgressIndicator());
  }

  public static File persist(
      final IncrementalIndex index, final Interval dataInterval, File outDir, ProgressIndicator progress
  ) throws IOException
  {
    if (index.isEmpty()) {
      throw new IAE("Trying to persist an empty index!");
    }

    final long firstTimestamp = index.getMinTime().getMillis();
    final long lastTimestamp = index.getMaxTime().getMillis();
    if (!(dataInterval.contains(firstTimestamp) && dataInterval.contains(lastTimestamp))) {
      throw new IAE(
          "interval[%s] does not encapsulate the full range of timestamps[%s, %s]",
          dataInterval,
          new DateTime(firstTimestamp),
          new DateTime(lastTimestamp)
      );
    }

    if (!outDir.exists()) {
      outDir.mkdirs();
    }
    if (!outDir.isDirectory()) {
      throw new ISE("Can only persist to directories, [%s] wasn't a directory", outDir);
    }

    log.info("Starting persist for interval[%s], rows[%,d]", dataInterval, index.size());
    return merge(
        Arrays.<IndexableAdapter>asList(new IncrementalIndexAdapter(dataInterval, index)),
        index.getMetricAggs(),
        outDir,
        progress
    );
  }

  public static File mergeQueryableIndex(
      List<QueryableIndex> indexes, final AggregatorFactory[] metricAggs, File outDir
  ) throws IOException
  {
    return mergeQueryableIndex(indexes, metricAggs, outDir, new NoopProgressIndicator());
  }

  public static File mergeQueryableIndex(
      List<QueryableIndex> indexes, final AggregatorFactory[] metricAggs, File outDir, ProgressIndicator progress
  ) throws IOException
  {
    return merge(
        Lists.transform(
            indexes,
            new Function<QueryableIndex, IndexableAdapter>()
            {
              @Override
              public IndexableAdapter apply(final QueryableIndex input)
              {
                return new QueryableIndexIndexableAdapter(input);
              }
            }
        ),
        metricAggs,
        outDir,
        progress
    );
  }

  public static File merge(
      List<IndexableAdapter> indexes, final AggregatorFactory[] metricAggs, File outDir
  ) throws IOException
  {
    return merge(indexes, metricAggs, outDir, new NoopProgressIndicator());
  }

  public static File merge(
      List<IndexableAdapter> indexes, final AggregatorFactory[] metricAggs, File outDir, ProgressIndicator progress
  ) throws IOException
  {
    FileUtils.deleteDirectory(outDir);
    if (!outDir.mkdirs()) {
      throw new ISE("Couldn't make outdir[%s].", outDir);
    }

    final AggregatorFactory[] lowerCaseMetricAggs = new AggregatorFactory[metricAggs.length];
    for (int i = 0; i < metricAggs.length; i++) {
      lowerCaseMetricAggs[i] = new ToLowerCaseAggregatorFactory(metricAggs[i]);
    }

    final List<String> mergedDimensions = mergeIndexed(
        Lists.transform(
            indexes,
            new Function<IndexableAdapter, Iterable<String>>()
            {
              @Override
              public Iterable<String> apply(@Nullable IndexableAdapter input)
              {
                return Iterables.transform(
                    input.getAvailableDimensions(),
                    new Function<String, String>()
                    {
                      @Override
                      public String apply(@Nullable String input)
                      {
                        return input.toLowerCase();
                      }
                    }
                );
              }
            }
        )
    );
    final List<String> mergedMetrics = Lists.transform(
        mergeIndexed(
            Lists.<Iterable<String>>newArrayList(
                FunctionalIterable
                    .create(indexes)
                    .transform(
                        new Function<IndexableAdapter, Iterable<String>>()
                        {
                          @Override
                          public Iterable<String> apply(@Nullable IndexableAdapter input)
                          {
                            return Iterables.transform(
                                input.getAvailableMetrics(),
                                new Function<String, String>()
                                {
                                  @Override
                                  public String apply(@Nullable String input)
                                  {
                                    return input.toLowerCase();
                                  }
                                }
                            );
                          }
                        }
                    )
                    .concat(Arrays.<Iterable<String>>asList(new AggFactoryStringIndexed(lowerCaseMetricAggs)))
            )
        ),
        new Function<String, String>()
        {
          @Override
          public String apply(@Nullable String input)
          {
            return input.toLowerCase();
          }
        }
    );
    if (mergedMetrics.size() != lowerCaseMetricAggs.length) {
      throw new IAE("Bad number of metrics[%d], expected [%d]", mergedMetrics.size(), lowerCaseMetricAggs.length);
    }

    final AggregatorFactory[] sortedMetricAggs = new AggregatorFactory[mergedMetrics.size()];
    for (int i = 0; i < lowerCaseMetricAggs.length; i++) {
      AggregatorFactory metricAgg = lowerCaseMetricAggs[i];
      sortedMetricAggs[mergedMetrics.indexOf(metricAgg.getName())] = metricAgg;
    }

    for (int i = 0; i < mergedMetrics.size(); i++) {
      if (!sortedMetricAggs[i].getName().equals(mergedMetrics.get(i))) {
        throw new IAE(
            "Metric mismatch, index[%d] [%s] != [%s]",
            i,
            lowerCaseMetricAggs[i].getName(),
            mergedMetrics.get(i)
        );
      }
    }

    Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>> rowMergerFn = new Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>>()
    {
      @Override
      public Iterable<Rowboat> apply(
          @Nullable ArrayList<Iterable<Rowboat>> boats
      )
      {
        return CombiningIterable.create(
            new MergeIterable<Rowboat>(
                Ordering.<Rowboat>natural().nullsFirst(),
                boats
            ),
            Ordering.<Rowboat>natural().nullsFirst(),
            new RowboatMergeFunction(sortedMetricAggs)
        );
      }
    };

    return makeIndexFiles(indexes, outDir, progress, mergedDimensions, mergedMetrics, rowMergerFn);
  }

  public static File append(
      List<IndexableAdapter> indexes, File outDir
  ) throws IOException
  {
    return append(indexes, outDir, new NoopProgressIndicator());
  }

  public static File append(
      List<IndexableAdapter> indexes, File outDir, ProgressIndicator progress
  ) throws IOException
  {
    FileUtils.deleteDirectory(outDir);
    if (!outDir.mkdirs()) {
      throw new ISE("Couldn't make outdir[%s].", outDir);
    }

/*
    if (indexes.size() < 2) {
      throw new ISE("Too few indexes provided for append [%d].", indexes.size());
    }
*/

    final List<String> mergedDimensions = mergeIndexed(
        Lists.transform(
            indexes,
            new Function<IndexableAdapter, Iterable<String>>()
            {
              @Override
              public Iterable<String> apply(@Nullable IndexableAdapter input)
              {
                return Iterables.transform(
                    input.getAvailableDimensions(),
                    new Function<String, String>()
                    {
                      @Override
                      public String apply(@Nullable String input)
                      {
                        return input.toLowerCase();
                      }
                    }
                );
              }
            }
        )
    );
    final List<String> mergedMetrics = mergeIndexed(
        Lists.transform(
            indexes,
            new Function<IndexableAdapter, Iterable<String>>()
            {
              @Override
              public Iterable<String> apply(@Nullable IndexableAdapter input)
              {
                return Iterables.transform(
                    input.getAvailableMetrics(),
                    new Function<String, String>()
                    {
                      @Override
                      public String apply(@Nullable String input)
                      {
                        return input.toLowerCase();
                      }
                    }
                );
              }
            }
        )
    );

    Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>> rowMergerFn = new Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>>()
    {
      @Override
      public Iterable<Rowboat> apply(
          @Nullable final ArrayList<Iterable<Rowboat>> boats
      )
      {
        return new MergeIterable<Rowboat>(
            Ordering.<Rowboat>natural().nullsFirst(),
            boats
        );
      }
    };

    return makeIndexFiles(indexes, outDir, progress, mergedDimensions, mergedMetrics, rowMergerFn);
  }

  private static File makeIndexFiles(
      final List<IndexableAdapter> indexes,
      final File outDir,
      final ProgressIndicator progress,
      final List<String> mergedDimensions,
      final List<String> mergedMetrics,
      final Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>> rowMergerFn
  ) throws IOException
  {
    Map<String, String> metricTypes = Maps.newTreeMap(Ordering.<String>natural().nullsFirst());
    for (IndexableAdapter adapter : indexes) {
      for (String metric : adapter.getAvailableMetrics()) {
        metricTypes.put(metric, adapter.getMetricType(metric));
      }
    }
    final Interval dataInterval;
    File v8OutDir = new File(outDir, "v8-tmp");
    v8OutDir.mkdirs();

    /*************  Main index.drd file **************/
    progress.progress();
    long startTime = System.currentTimeMillis();
    File indexFile = new File(v8OutDir, "index.drd");

    FileOutputStream fileOutputStream = null;
    FileChannel channel = null;
    try {
      fileOutputStream = new FileOutputStream(indexFile);
      channel = fileOutputStream.getChannel();
      channel.write(ByteBuffer.wrap(new byte[]{IndexIO.V8_VERSION}));

      GenericIndexed.fromIterable(mergedDimensions, GenericIndexed.stringStrategy).writeToChannel(channel);
      GenericIndexed.fromIterable(mergedMetrics, GenericIndexed.stringStrategy).writeToChannel(channel);

      DateTime minTime = new DateTime(Long.MAX_VALUE);
      DateTime maxTime = new DateTime(0l);

      for (IndexableAdapter index : indexes) {
        minTime = JodaUtils.minDateTime(minTime, index.getDataInterval().getStart());
        maxTime = JodaUtils.maxDateTime(maxTime, index.getDataInterval().getEnd());
      }

      dataInterval = new Interval(minTime, maxTime);
      serializerUtils.writeString(channel, String.format("%s/%s", minTime, maxTime));
    }
    finally {
      Closeables.closeQuietly(channel);
      channel = null;
      Closeables.closeQuietly(fileOutputStream);
      fileOutputStream = null;
    }
    IndexIO.checkFileSize(indexFile);
    log.info("outDir[%s] completed index.drd in %,d millis.", v8OutDir, System.currentTimeMillis() - startTime);

    /************* Setup Dim Conversions **************/
    progress.progress();
    startTime = System.currentTimeMillis();

    IOPeon ioPeon = new TmpFileIOPeon();
    ArrayList<FileOutputSupplier> dimOuts = Lists.newArrayListWithCapacity(mergedDimensions.size());
    Map<String, Integer> dimensionCardinalities = Maps.newHashMap();
    ArrayList<Map<String, IntBuffer>> dimConversions = Lists.newArrayListWithCapacity(indexes.size());

    for (IndexableAdapter index : indexes) {
      dimConversions.add(Maps.<String, IntBuffer>newHashMap());
    }

    for (String dimension : mergedDimensions) {
      final GenericIndexedWriter<String> writer = new GenericIndexedWriter<String>(
          ioPeon, dimension, GenericIndexed.stringStrategy
      );
      writer.open();

      List<Indexed<String>> dimValueLookups = Lists.newArrayListWithCapacity(indexes.size());
      DimValueConverter[] converters = new DimValueConverter[indexes.size()];
      for (int i = 0; i < indexes.size(); i++) {
        Indexed<String> dimValues = indexes.get(i).getDimValueLookup(dimension);
        if (dimValues != null) {
          dimValueLookups.add(dimValues);
          converters[i] = new DimValueConverter(dimValues);
        }
      }

      Iterable<String> dimensionValues = CombiningIterable.createSplatted(
          Iterables.transform(
              dimValueLookups,
              new Function<Indexed<String>, Iterable<String>>()
              {
                @Override
                public Iterable<String> apply(@Nullable Indexed<String> indexed)
                {
                  return Iterables.transform(
                      indexed,
                      new Function<String, String>()
                      {
                        @Override
                        public String apply(@Nullable String input)
                        {
                          return (input == null) ? "" : input;
                        }
                      }
                  );
                }
              }
          )
          ,
          Ordering.<String>natural().nullsFirst()
      );

      int count = 0;
      for (String value : dimensionValues) {
        value = value == null ? "" : value;
        writer.write(value);

        for (int i = 0; i < indexes.size(); i++) {
          DimValueConverter converter = converters[i];
          if (converter != null) {
            converter.convert(value, count);
          }
        }

        ++count;
      }
      dimensionCardinalities.put(dimension, count);

      FileOutputSupplier dimOut = new FileOutputSupplier(IndexIO.makeDimFile(v8OutDir, dimension), true);
      dimOuts.add(dimOut);

      writer.close();
      serializerUtils.writeString(dimOut, dimension);
      ByteStreams.copy(writer.combineStreams(), dimOut);
      for (int i = 0; i < indexes.size(); ++i) {
        DimValueConverter converter = converters[i];
        if (converter != null) {
          dimConversions.get(i).put(dimension, converters[i].getConversionBuffer());
        }
      }

      ioPeon.cleanup();
    }
    log.info("outDir[%s] completed dim conversions in %,d millis.", v8OutDir, System.currentTimeMillis() - startTime);

    /************* Walk through data sets and merge them *************/
    progress.progress();
    startTime = System.currentTimeMillis();

    ArrayList<Iterable<Rowboat>> boats = Lists.newArrayListWithCapacity(indexes.size());

    for (int i = 0; i < indexes.size(); ++i) {
      final IndexableAdapter adapter = indexes.get(i);

      final int[] dimLookup = new int[mergedDimensions.size()];
      int count = 0;
      for (String dim : adapter.getAvailableDimensions()) {
        dimLookup[count] = mergedDimensions.indexOf(dim.toLowerCase());
        count++;
      }

      final int[] metricLookup = new int[mergedMetrics.size()];
      count = 0;
      for (String metric : adapter.getAvailableMetrics()) {
        metricLookup[count] = mergedMetrics.indexOf(metric);
        count++;
      }

      boats.add(
          new MMappedIndexRowIterable(
              Iterables.transform(
                  indexes.get(i).getRows(),
                  new Function<Rowboat, Rowboat>()
                  {
                    @Override
                    public Rowboat apply(@Nullable Rowboat input)
                    {
                      int[][] newDims = new int[mergedDimensions.size()][];
                      int j = 0;
                      for (int[] dim : input.getDims()) {
                        newDims[dimLookup[j]] = dim;
                        j++;
                      }

                      Object[] newMetrics = new Object[mergedMetrics.size()];
                      j = 0;
                      for (Object met : input.getMetrics()) {
                        newMetrics[metricLookup[j]] = met;
                        j++;
                      }

                      return new Rowboat(
                          input.getTimestamp(),
                          newDims,
                          newMetrics,
                          input.getRowNum(),
                          input.getDescriptions()
                      );
                    }
                  }
              )
              , mergedDimensions, dimConversions.get(i), i
          )
      );
    }

    Iterable<Rowboat> theRows = rowMergerFn.apply(boats);

    CompressedLongsSupplierSerializer timeWriter = CompressedLongsSupplierSerializer.create(
        ioPeon, "little_end_time", IndexIO.BYTE_ORDER
    );

    timeWriter.open();

    ArrayList<VSizeIndexedWriter> forwardDimWriters = Lists.newArrayListWithCapacity(mergedDimensions.size());
    for (String dimension : mergedDimensions) {
      VSizeIndexedWriter writer = new VSizeIndexedWriter(ioPeon, dimension, dimensionCardinalities.get(dimension));
      writer.open();
      forwardDimWriters.add(writer);
    }

    ArrayList<MetricColumnSerializer> metWriters = Lists.newArrayListWithCapacity(mergedMetrics.size());
    for (Map.Entry<String, String> entry : metricTypes.entrySet()) {
      String metric = entry.getKey();
      String typeName = entry.getValue();
      if ("float".equals(typeName)) {
        metWriters.add(new FloatMetricColumnSerializer(metric, v8OutDir, ioPeon));
      } else {
        ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);

        if (serde == null) {
          throw new ISE("Unknown type[%s]", typeName);
        }

        metWriters.add(new ComplexMetricColumnSerializer(metric, v8OutDir, ioPeon, serde));
      }
    }
    for (MetricColumnSerializer metWriter : metWriters) {
      metWriter.open();
    }

    int rowCount = 0;
    long time = System.currentTimeMillis();
    List<IntBuffer> rowNumConversions = Lists.newArrayListWithCapacity(indexes.size());
    for (IndexableAdapter index : indexes) {
      int[] arr = new int[index.getNumRows()];
      Arrays.fill(arr, INVALID_ROW);
      rowNumConversions.add(IntBuffer.wrap(arr));
    }

    final Map<String, String> descriptions = Maps.newHashMap();
    for (Rowboat theRow : theRows) {
      progress.progress();
      timeWriter.add(theRow.getTimestamp());

      final Object[] metrics = theRow.getMetrics();
      for (int i = 0; i < metrics.length; ++i) {
        metWriters.get(i).serialize(metrics[i]);
      }

      int[][] dims = theRow.getDims();
      for (int i = 0; i < dims.length; ++i) {
        List<Integer> listToWrite = (i >= dims.length || dims[i] == null)
                                    ? null
                                    : Ints.asList(dims[i]);
        forwardDimWriters.get(i).write(listToWrite);
      }

      for (Map.Entry<Integer, TreeSet<Integer>> comprisedRow : theRow.getComprisedRows().entrySet()) {
        final IntBuffer conversionBuffer = rowNumConversions.get(comprisedRow.getKey());

        for (Integer rowNum : comprisedRow.getValue()) {
          while (conversionBuffer.position() < rowNum) {
            conversionBuffer.put(INVALID_ROW);
          }
          conversionBuffer.put(rowCount);
        }
      }

      if ((++rowCount % 500000) == 0) {
        log.info(
            "outDir[%s] walked 500,000/%,d rows in %,d millis.", v8OutDir, rowCount, System.currentTimeMillis() - time
        );
        time = System.currentTimeMillis();
      }

      descriptions.putAll(theRow.getDescriptions());
    }

    for (IntBuffer rowNumConversion : rowNumConversions) {
      rowNumConversion.rewind();
    }

    final File timeFile = IndexIO.makeTimeFile(v8OutDir, IndexIO.BYTE_ORDER);
    timeFile.delete();
    OutputSupplier<FileOutputStream> out = Files.newOutputStreamSupplier(timeFile, true);
    timeWriter.closeAndConsolidate(out);
    IndexIO.checkFileSize(timeFile);

    for (int i = 0; i < mergedDimensions.size(); ++i) {
      forwardDimWriters.get(i).close();
      ByteStreams.copy(forwardDimWriters.get(i).combineStreams(), dimOuts.get(i));
    }

    for (MetricColumnSerializer metWriter : metWriters) {
      metWriter.close();
    }

    ioPeon.cleanup();
    log.info(
        "outDir[%s] completed walk through of %,d rows in %,d millis.",
        v8OutDir,
        rowCount,
        System.currentTimeMillis() - startTime
    );

    /************ Create Inverted Indexes *************/
    startTime = System.currentTimeMillis();

    final File invertedFile = new File(v8OutDir, "inverted.drd");
    Files.touch(invertedFile);
    out = Files.newOutputStreamSupplier(invertedFile, true);

    final File geoFile = new File(v8OutDir, "spatial.drd");
    Files.touch(geoFile);
    OutputSupplier<FileOutputStream> spatialOut = Files.newOutputStreamSupplier(geoFile, true);

    for (int i = 0; i < mergedDimensions.size(); ++i) {
      long dimStartTime = System.currentTimeMillis();
      String dimension = mergedDimensions.get(i);

      File dimOutFile = dimOuts.get(i).getFile();
      final MappedByteBuffer dimValsMapped = Files.map(dimOutFile);

      if (!dimension.equals(serializerUtils.readString(dimValsMapped))) {
        throw new ISE("dimensions[%s] didn't equate!?  This is a major WTF moment.", dimension);
      }
      Indexed<String> dimVals = GenericIndexed.read(dimValsMapped, GenericIndexed.stringStrategy);
      log.info("Starting dimension[%s] with cardinality[%,d]", dimension, dimVals.size());

      GenericIndexedWriter<ImmutableConciseSet> writer = new GenericIndexedWriter<ImmutableConciseSet>(
          ioPeon, dimension, ConciseCompressedIndexedInts.objectStrategy
      );
      writer.open();

      boolean isSpatialDim = "spatial".equals(descriptions.get(dimension));
      ByteBufferWriter<ImmutableRTree> spatialWriter = null;
      RTree tree = null;
      IOPeon spatialIoPeon = new TmpFileIOPeon();
      if (isSpatialDim) {
        spatialWriter = new ByteBufferWriter<ImmutableRTree>(
            spatialIoPeon, dimension, IndexedRTree.objectStrategy
        );
        spatialWriter.open();
        tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50));
      }

      for (String dimVal : IndexedIterable.create(dimVals)) {
        progress.progress();
        List<Iterable<Integer>> convertedInverteds = Lists.newArrayListWithCapacity(indexes.size());
        for (int j = 0; j < indexes.size(); ++j) {
          convertedInverteds.add(
              new ConvertingIndexedInts(
                  indexes.get(j).getInverteds(dimension, dimVal), rowNumConversions.get(j)
              )
          );
        }

        ConciseSet bitset = new ConciseSet();
        for (Integer row : CombiningIterable.createSplatted(
            convertedInverteds,
            Ordering.<Integer>natural().nullsFirst()
        )) {
          if (row != INVALID_ROW) {
            bitset.add(row);
          }
        }

        writer.write(ImmutableConciseSet.newImmutableFromMutable(bitset));

        if (isSpatialDim && dimVal != null) {
          List<String> stringCoords = Lists.newArrayList(SPLITTER.split(dimVal));
          float[] coords = new float[stringCoords.size()];
          for (int j = 0; j < coords.length; j++) {
            coords[j] = Float.valueOf(stringCoords.get(j));
          }
          tree.insert(coords, bitset);
        }
      }
      writer.close();

      serializerUtils.writeString(out, dimension);
      ByteStreams.copy(writer.combineStreams(), out);
      ioPeon.cleanup();

      log.info("Completed dimension[%s] in %,d millis.", dimension, System.currentTimeMillis() - dimStartTime);

      if (isSpatialDim) {
        spatialWriter.write(ImmutableRTree.newImmutableFromMutable(tree));
        spatialWriter.close();

        serializerUtils.writeString(spatialOut, dimension);
        ByteStreams.copy(spatialWriter.combineStreams(), spatialOut);
        spatialIoPeon.cleanup();
      }

    }

    log.info("outDir[%s] completed inverted.drd in %,d millis.", v8OutDir, System.currentTimeMillis() - startTime);

    final ArrayList<String> expectedFiles = Lists.newArrayList(
        Iterables.concat(
            Arrays.asList(
                "index.drd", "inverted.drd", "spatial.drd", String.format("time_%s.drd", IndexIO.BYTE_ORDER)
            ),
            Iterables.transform(mergedDimensions, GuavaUtils.formatFunction("dim_%s.drd")),
            Iterables.transform(
                mergedMetrics, GuavaUtils.formatFunction(String.format("met_%%s_%s.drd", IndexIO.BYTE_ORDER))
            )
        )
    );

    Map<String, File> files = Maps.newLinkedHashMap();
    for (String fileName : expectedFiles) {
      files.put(fileName, new File(v8OutDir, fileName));
    }

    File smooshDir = new File(v8OutDir, "smoosher");
    smooshDir.mkdir();

    for (Map.Entry<String, File> entry : Smoosh.smoosh(v8OutDir, smooshDir, files).entrySet()) {
      entry.getValue().delete();
    }

    for (File file : smooshDir.listFiles()) {
      Files.move(file, new File(v8OutDir, file.getName()));
    }

    if (!smooshDir.delete()) {
      log.info("Unable to delete temporary dir[%s], contains[%s]", smooshDir, Arrays.asList(smooshDir.listFiles()));
      throw new IOException(String.format("Unable to delete temporary dir[%s]", smooshDir));
    }

    createIndexDrdFile(
        IndexIO.V8_VERSION,
        v8OutDir,
        GenericIndexed.fromIterable(mergedDimensions, GenericIndexed.stringStrategy),
        GenericIndexed.fromIterable(mergedMetrics, GenericIndexed.stringStrategy),
        dataInterval
    );

    IndexIO.DefaultIndexIOHandler.convertV8toV9(v8OutDir, outDir);
    FileUtils.deleteDirectory(v8OutDir);

    return outDir;
  }

  private static <T extends Comparable> ArrayList<T> mergeIndexed(final List<Iterable<T>> indexedLists)
  {
    Set<T> retVal = Sets.newTreeSet(Ordering.<T>natural().nullsFirst());

    for (Iterable<T> indexedList : indexedLists) {
      for (T val : indexedList) {
        retVal.add(val);
      }
    }

    return Lists.newArrayList(retVal);
  }

  public static void createIndexDrdFile(
      byte versionId,
      File inDir,
      GenericIndexed<String> availableDimensions,
      GenericIndexed<String> availableMetrics,
      Interval dataInterval
  ) throws IOException
  {
    File indexFile = new File(inDir, "index.drd");

    FileChannel channel = null;
    try {
      channel = new FileOutputStream(indexFile).getChannel();
      channel.write(ByteBuffer.wrap(new byte[]{versionId}));

      availableDimensions.writeToChannel(channel);
      availableMetrics.writeToChannel(channel);
      serializerUtils.writeString(
          channel, String.format("%s/%s", dataInterval.getStart(), dataInterval.getEnd())
      );
    }
    finally {
      Closeables.closeQuietly(channel);
      channel = null;
    }
    IndexIO.checkFileSize(indexFile);
  }

  private static class DimValueConverter
  {
    private final Indexed<String> dimSet;
    private final IntBuffer conversionBuf;

    private int currIndex;
    private String lastVal = null;

    DimValueConverter(
        Indexed<String> dimSet
    )
    {
      this.dimSet = dimSet;
      conversionBuf = ByteBuffer.allocateDirect(dimSet.size() * Ints.BYTES).asIntBuffer();

      currIndex = 0;
    }

    public void convert(String value, int index)
    {
      if (dimSet.size() == 0) {
        return;
      }
      if (lastVal != null) {
        if (value.compareTo(lastVal) <= 0) {
          throw new ISE("Value[%s] is less than the last value[%s] I have, cannot be.", value, lastVal);
        }
        return;
      }
      String currValue = dimSet.get(currIndex);

      while (currValue == null) {
        conversionBuf.position(conversionBuf.position() + 1);
        ++currIndex;
        if (currIndex == dimSet.size()) {
          lastVal = value;
          return;
        }
        currValue = dimSet.get(currIndex);
      }

      if (Objects.equal(currValue, value)) {
        conversionBuf.put(index);
        ++currIndex;
        if (currIndex == dimSet.size()) {
          lastVal = value;
        }
      } else if (currValue.compareTo(value) < 0) {
        throw new ISE(
            "Skipped currValue[%s], currIndex[%,d]; incoming value[%s], index[%,d]", currValue, currIndex, value, index
        );
      }
    }

    public IntBuffer getConversionBuffer()
    {
      if (currIndex != conversionBuf.limit() || conversionBuf.hasRemaining()) {
        throw new ISE(
            "Asked for incomplete buffer.  currIndex[%,d] != buf.limit[%,d]", currIndex, conversionBuf.limit()
        );
      }
      return (IntBuffer) conversionBuf.asReadOnlyBuffer().rewind();
    }
  }

  private static class ConvertingIndexedInts implements Iterable<Integer>
  {
    private final IndexedInts baseIndex;
    private final IntBuffer conversionBuffer;

    public ConvertingIndexedInts(
        IndexedInts baseIndex,
        IntBuffer conversionBuffer
    )
    {
      this.baseIndex = baseIndex;
      this.conversionBuffer = conversionBuffer;
    }

    public int size()
    {
      return baseIndex.size();
    }

    public int get(int index)
    {
      return conversionBuffer.get(baseIndex.get(index));
    }

    @Override
    public Iterator<Integer> iterator()
    {
      return Iterators.transform(
          baseIndex.iterator(),
          new Function<Integer, Integer>()
          {
            @Override
            public Integer apply(@Nullable Integer input)
            {
              return conversionBuffer.get(input);
            }
          }
      );
    }
  }

  private static class MMappedIndexRowIterable implements Iterable<Rowboat>
  {
    private final Iterable<Rowboat> index;
    private final List<String> convertedDims;
    private final Map<String, IntBuffer> converters;
    private final int indexNumber;

    MMappedIndexRowIterable(
        Iterable<Rowboat> index,
        List<String> convertedDims,
        Map<String, IntBuffer> converters,
        int indexNumber
    )
    {
      this.index = index;
      this.convertedDims = convertedDims;
      this.converters = converters;
      this.indexNumber = indexNumber;
    }

    public Iterable<Rowboat> getIndex()
    {
      return index;
    }

    public List<String> getConvertedDims()
    {
      return convertedDims;
    }

    public Map<String, IntBuffer> getConverters()
    {
      return converters;
    }

    public int getIndexNumber()
    {
      return indexNumber;
    }

    @Override
    public Iterator<Rowboat> iterator()
    {
      return Iterators.transform(
          index.iterator(),
          new Function<Rowboat, Rowboat>()
          {
            int rowCount = 0;

            @Override
            public Rowboat apply(@Nullable Rowboat input)
            {
              int[][] dims = input.getDims();
              int[][] newDims = new int[convertedDims.size()][];
              for (int i = 0; i < convertedDims.size(); ++i) {
                IntBuffer converter = converters.get(convertedDims.get(i));

                if (converter == null) {
                  continue;
                }

                if (i >= dims.length || dims[i] == null) {
                  continue;
                }

                newDims[i] = new int[dims[i].length];

                for (int j = 0; j < dims[i].length; ++j) {
                  if (!converter.hasRemaining()) {
                    log.error("Converter mismatch! wtfbbq!");
                  }
                  newDims[i][j] = converter.get(dims[i][j]);
                }
              }

              final Rowboat retVal = new Rowboat(
                  input.getTimestamp(),
                  newDims,
                  input.getMetrics(),
                  input.getRowNum(),
                  input.getDescriptions()
              );

              retVal.addRow(indexNumber, input.getRowNum());

              return retVal;
            }
          }
      );
    }
  }

  private static class AggFactoryStringIndexed implements Indexed<String>
  {
    private final AggregatorFactory[] metricAggs;

    public AggFactoryStringIndexed(AggregatorFactory[] metricAggs) {this.metricAggs = metricAggs;}

    @Override
    public Class<? extends String> getClazz()
    {
      return String.class;
    }

    @Override
    public int size()
    {
      return metricAggs.length;
    }

    @Override
    public String get(int index)
    {
      return metricAggs[index].getName();
    }

    @Override
    public int indexOf(String value)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<String> iterator()
    {
      return IndexedIterable.create(this).iterator();
    }
  }

  private static class RowboatMergeFunction implements BinaryFn<Rowboat, Rowboat, Rowboat>
  {
    private final AggregatorFactory[] metricAggs;

    public RowboatMergeFunction(AggregatorFactory[] metricAggs)
    {
      this.metricAggs = metricAggs;
    }

    @Override
    public Rowboat apply(Rowboat lhs, Rowboat rhs)
    {
      if (lhs == null) {
        return rhs;
      }
      if (rhs == null) {
        return lhs;
      }

      Object[] metrics = new Object[metricAggs.length];
      Object[] lhsMetrics = lhs.getMetrics();
      Object[] rhsMetrics = rhs.getMetrics();

      for (int i = 0; i < metrics.length; ++i) {
        metrics[i] = metricAggs[i].combine(lhsMetrics[i], rhsMetrics[i]);
      }

      final Rowboat retVal = new Rowboat(
          lhs.getTimestamp(),
          lhs.getDims(),
          metrics,
          lhs.getRowNum(),
          lhs.getDescriptions()
      );

      for (Rowboat rowboat : Arrays.asList(lhs, rhs)) {
        for (Map.Entry<Integer, TreeSet<Integer>> entry : rowboat.getComprisedRows().entrySet()) {
          for (Integer rowNum : entry.getValue()) {
            retVal.addRow(entry.getKey(), rowNum);
          }
        }
      }

      return retVal;
    }
  }

  public static interface ProgressIndicator
  {
    public void progress();
  }

  private static class NoopProgressIndicator implements ProgressIndicator
  {
    @Override
    public void progress() {}
  }
}
