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
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.Ints;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
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
import io.druid.collections.CombiningIterable;
import io.druid.common.guava.FileOutputSupplier;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.SerializerUtils;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.column.BitmapIndexSeeker;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferWriter;
import io.druid.segment.data.CompressedLongsSupplierSerializer;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.TmpFileIOPeon;
import io.druid.segment.data.VSizeIndexedWriter;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexAdapter;
import io.druid.segment.serde.ComplexMetricColumnSerializer;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
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

  private static final ObjectMapper mapper;

  static {
    final Injector injector = GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bind(binder, "druid.processing.bitmap", BitmapSerdeFactory.class);
              }
            }
        )
    );
    mapper = injector.getInstance(ObjectMapper.class);
  }


  public static File persist(
      final IncrementalIndex index,
      File outDir,
      Map<String, Object> segmentMetadata,
      IndexSpec indexSpec
  ) throws IOException
  {
    return persist(index, index.getInterval(), outDir, segmentMetadata, indexSpec);
  }

  /**
   * This is *not* thread-safe and havok will ensue if this is called and writes are still occurring
   * on the IncrementalIndex object.
   *
   * @param index        the IncrementalIndex to persist
   * @param dataInterval the Interval that the data represents
   * @param outDir       the directory to persist the data to
   *
   * @return the index output directory
   *
   * @throws java.io.IOException if an IO error occurs persisting the index
   */
  public static File persist(
      final IncrementalIndex index,
      final Interval dataInterval,
      File outDir,
      Map<String, Object> segmentMetadata,
      IndexSpec indexSpec
  ) throws IOException
  {
    return persist(index, dataInterval, outDir, segmentMetadata, indexSpec, new BaseProgressIndicator());
  }

  public static File persist(
      final IncrementalIndex index,
      final Interval dataInterval,
      File outDir,
      Map<String, Object> segmentMetadata,
      IndexSpec indexSpec,
      ProgressIndicator progress
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
        Arrays.<IndexableAdapter>asList(
            new IncrementalIndexAdapter(
                dataInterval,
                index,
                indexSpec.getBitmapSerdeFactory().getBitmapFactory()
            )
        ),
        index.getMetricAggs(),
        outDir,
        segmentMetadata,
        indexSpec,
        progress
    );
  }

  public static File mergeQueryableIndex(
      List<QueryableIndex> indexes, final AggregatorFactory[] metricAggs, File outDir, IndexSpec indexSpec
  ) throws IOException
  {
    return mergeQueryableIndex(indexes, metricAggs, outDir, indexSpec, new BaseProgressIndicator());
  }

  public static File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException
  {
    // We are materializing the list for performance reasons. Lists.transform
    // only creates a "view" of the original list, meaning the function gets
    // applied every time you access an element.
    List<IndexableAdapter> indexAdapteres = Lists.newArrayList(
        Iterables.transform(
            indexes,
            new Function<QueryableIndex, IndexableAdapter>()
            {
              @Override
              public IndexableAdapter apply(final QueryableIndex input)
              {
                return new QueryableIndexIndexableAdapter(input);
              }
            }
        )
    );
    return merge(
        indexAdapteres,
        metricAggs,
        outDir,
        null,
        indexSpec,
        progress
    );
  }

  public static File merge(
      List<IndexableAdapter> indexes,
      final AggregatorFactory[] metricAggs,
      File outDir,
      Map<String, Object> segmentMetadata,
      IndexSpec indexSpec
  ) throws IOException
  {
    return merge(indexes, metricAggs, outDir, segmentMetadata, indexSpec, new BaseProgressIndicator());
  }

  public static File merge(
      List<IndexableAdapter> indexes,
      final AggregatorFactory[] metricAggs,
      File outDir,
      Map<String, Object> segmentMetadata,
      IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException
  {
    FileUtils.deleteDirectory(outDir);
    if (!outDir.mkdirs()) {
      throw new ISE("Couldn't make outdir[%s].", outDir);
    }

    final List<String> mergedDimensions = mergeIndexed(
        Lists.transform(
            indexes,
            new Function<IndexableAdapter, Iterable<String>>()
            {
              @Override
              public Iterable<String> apply(@Nullable IndexableAdapter input)
              {
                return input.getDimensionNames();
              }
            }
        )
    );
    final List<String> mergedMetrics = Lists.transform(
        mergeIndexed(
            Lists.newArrayList(
                FunctionalIterable
                    .create(indexes)
                    .transform(
                        new Function<IndexableAdapter, Iterable<String>>()
                        {
                          @Override
                          public Iterable<String> apply(@Nullable IndexableAdapter input)
                          {
                            return input.getMetricNames();
                          }
                        }
                    )
                    .concat(Arrays.<Iterable<String>>asList(new AggFactoryStringIndexed(metricAggs)))
            )
        ),
        new Function<String, String>()
        {
          @Override
          public String apply(@Nullable String input)
          {
            return input;
          }
        }
    );
    if (mergedMetrics.size() != metricAggs.length) {
      throw new IAE("Bad number of metrics[%d], expected [%d]", mergedMetrics.size(), metricAggs.length);
    }

    final AggregatorFactory[] sortedMetricAggs = new AggregatorFactory[mergedMetrics.size()];
    for (int i = 0; i < metricAggs.length; i++) {
      AggregatorFactory metricAgg = metricAggs[i];
      sortedMetricAggs[mergedMetrics.indexOf(metricAgg.getName())] = metricAgg;
    }

    for (int i = 0; i < mergedMetrics.size(); i++) {
      if (!sortedMetricAggs[i].getName().equals(mergedMetrics.get(i))) {
        throw new IAE(
            "Metric mismatch, index[%d] [%s] != [%s]",
            i,
            metricAggs[i].getName(),
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

    return makeIndexFiles(
        indexes,
        outDir,
        progress,
        mergedDimensions,
        mergedMetrics,
        segmentMetadata,
        rowMergerFn,
        indexSpec
    );
  }

  // Faster than IndexMaker
  public static File convert(final File inDir, final File outDir, final IndexSpec indexSpec) throws IOException
  {
    return convert(inDir, outDir, indexSpec, new BaseProgressIndicator());
  }

  public static File convert(
      final File inDir, final File outDir, final IndexSpec indexSpec, final ProgressIndicator progress
  ) throws IOException
  {
    try (QueryableIndex index = IndexIO.loadIndex(inDir)) {
      final IndexableAdapter adapter = new QueryableIndexIndexableAdapter(index);
      return makeIndexFiles(
          ImmutableList.of(adapter),
          outDir,
          progress,
          Lists.newArrayList(adapter.getDimensionNames()),
          Lists.newArrayList(adapter.getMetricNames()),
          null,
          new Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>>()
          {
            @Nullable
            @Override
            public Iterable<Rowboat> apply(ArrayList<Iterable<Rowboat>> input)
            {
              return input.get(0);
            }
          },
          indexSpec
      );
    }
  }

  public static File append(
      List<IndexableAdapter> indexes, File outDir, IndexSpec indexSpec
  ) throws IOException
  {
    return append(indexes, outDir, indexSpec, new BaseProgressIndicator());
  }

  public static File append(
      List<IndexableAdapter> indexes, File outDir, IndexSpec indexSpec, ProgressIndicator progress
  ) throws IOException
  {
    FileUtils.deleteDirectory(outDir);
    if (!outDir.mkdirs()) {
      throw new ISE("Couldn't make outdir[%s].", outDir);
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
                    input.getDimensionNames(),
                    new Function<String, String>()
                    {
                      @Override
                      public String apply(@Nullable String input)
                      {
                        return input;
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
                    input.getMetricNames(),
                    new Function<String, String>()
                    {
                      @Override
                      public String apply(@Nullable String input)
                      {
                        return input;
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

    return makeIndexFiles(indexes, outDir, progress, mergedDimensions, mergedMetrics, null, rowMergerFn, indexSpec);
  }

  private static File makeIndexFiles(
      final List<IndexableAdapter> indexes,
      final File outDir,
      final ProgressIndicator progress,
      final List<String> mergedDimensions,
      final List<String> mergedMetrics,
      final Map<String, Object> segmentMetadata,
      final Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>> rowMergerFn,
      final IndexSpec indexSpec
  ) throws IOException
  {
    final Map<String, ValueType> valueTypes = Maps.newTreeMap(Ordering.<String>natural().nullsFirst());
    final Map<String, String> metricTypeNames = Maps.newTreeMap(Ordering.<String>natural().nullsFirst());
    final Map<String, ColumnCapabilitiesImpl> columnCapabilities = Maps.newHashMap();

    for (IndexableAdapter adapter : indexes) {
      for (String dimension : adapter.getDimensionNames()) {
        ColumnCapabilitiesImpl mergedCapabilities = columnCapabilities.get(dimension);
        ColumnCapabilities capabilities = adapter.getCapabilities(dimension);
        if (mergedCapabilities == null) {
          mergedCapabilities = new ColumnCapabilitiesImpl();
          mergedCapabilities.setType(ValueType.STRING);
        }
        columnCapabilities.put(dimension, mergedCapabilities.merge(capabilities));
      }
      for (String metric : adapter.getMetricNames()) {
        ColumnCapabilitiesImpl mergedCapabilities = columnCapabilities.get(metric);
        ColumnCapabilities capabilities = adapter.getCapabilities(metric);
        if (mergedCapabilities == null) {
          mergedCapabilities = new ColumnCapabilitiesImpl();
        }
        columnCapabilities.put(metric, mergedCapabilities.merge(capabilities));

        valueTypes.put(metric, capabilities.getType());
        metricTypeNames.put(metric, adapter.getMetricType(metric));
      }
    }


    final Interval dataInterval;
    File v8OutDir = new File(outDir, "v8-tmp");
    v8OutDir.mkdirs();

    /*************  Main index.drd file **************/
    progress.progress();
    long startTime = System.currentTimeMillis();
    File indexFile = new File(v8OutDir, "index.drd");

    try (FileOutputStream fileOutputStream = new FileOutputStream(indexFile);
         FileChannel channel = fileOutputStream.getChannel()) {
      channel.write(ByteBuffer.wrap(new byte[]{IndexIO.V8_VERSION}));

      GenericIndexed.fromIterable(mergedDimensions, GenericIndexed.STRING_STRATEGY).writeToChannel(channel);
      GenericIndexed.fromIterable(mergedMetrics, GenericIndexed.STRING_STRATEGY).writeToChannel(channel);

      DateTime minTime = new DateTime(JodaUtils.MAX_INSTANT);
      DateTime maxTime = new DateTime(JodaUtils.MIN_INSTANT);

      for (IndexableAdapter index : indexes) {
        minTime = JodaUtils.minDateTime(minTime, index.getDataInterval().getStart());
        maxTime = JodaUtils.maxDateTime(maxTime, index.getDataInterval().getEnd());
      }

      dataInterval = new Interval(minTime, maxTime);
      serializerUtils.writeString(channel, String.format("%s/%s", minTime, maxTime));
      serializerUtils.writeString(channel, mapper.writeValueAsString(indexSpec.getBitmapSerdeFactory()));
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
          ioPeon, dimension, GenericIndexed.STRING_STRATEGY
      );
      writer.open();

      List<Indexed<String>> dimValueLookups = Lists.newArrayListWithCapacity(indexes.size());
      DimValueConverter[] converters = new DimValueConverter[indexes.size()];
      for (int i = 0; i < indexes.size(); i++) {
        Indexed<String> dimValues = indexes.get(i).getDimValueLookup(dimension);
        if (!isNullColumn(dimValues)) {
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
      for (String dim : adapter.getDimensionNames()) {
        dimLookup[count] = mergedDimensions.indexOf(dim);
        count++;
      }

      final int[] metricLookup = new int[mergedMetrics.size()];
      count = 0;
      for (String metric : adapter.getMetricNames()) {
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
                          input.getRowNum()
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
        ioPeon, "little_end_time", IndexIO.BYTE_ORDER, CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY
    );

    timeWriter.open();

    ArrayList<VSizeIndexedWriter> forwardDimWriters = Lists.newArrayListWithCapacity(mergedDimensions.size());
    for (String dimension : mergedDimensions) {
      VSizeIndexedWriter writer = new VSizeIndexedWriter(ioPeon, dimension, dimensionCardinalities.get(dimension));
      writer.open();
      forwardDimWriters.add(writer);
    }

    ArrayList<MetricColumnSerializer> metWriters = Lists.newArrayListWithCapacity(mergedMetrics.size());
    for (String metric : mergedMetrics) {
      ValueType type = valueTypes.get(metric);
      switch (type) {
        case LONG:
          metWriters.add(new LongMetricColumnSerializer(metric, v8OutDir, ioPeon));
          break;
        case FLOAT:
          metWriters.add(new FloatMetricColumnSerializer(metric, v8OutDir, ioPeon));
          break;
        case COMPLEX:
          final String typeName = metricTypeNames.get(metric);
          ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);

          if (serde == null) {
            throw new ISE("Unknown type[%s]", typeName);
          }

          metWriters.add(new ComplexMetricColumnSerializer(metric, v8OutDir, ioPeon, serde));
          break;
        default:
          throw new ISE("Unknown type[%s]", type);
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
      Indexed<String> dimVals = GenericIndexed.read(dimValsMapped, GenericIndexed.STRING_STRATEGY);
      log.info("Starting dimension[%s] with cardinality[%,d]", dimension, dimVals.size());

      final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();
      GenericIndexedWriter<ImmutableBitmap> writer = new GenericIndexedWriter<>(
          ioPeon, dimension, bitmapSerdeFactory.getObjectStrategy()
      );
      writer.open();

      boolean isSpatialDim = columnCapabilities.get(dimension).hasSpatialIndexes();
      ByteBufferWriter<ImmutableRTree> spatialWriter = null;
      RTree tree = null;
      IOPeon spatialIoPeon = new TmpFileIOPeon();
      if (isSpatialDim) {
        BitmapFactory bitmapFactory = bitmapSerdeFactory.getBitmapFactory();
        spatialWriter = new ByteBufferWriter<ImmutableRTree>(
            spatialIoPeon, dimension, new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapFactory)
        );
        spatialWriter.open();
        tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bitmapFactory), bitmapFactory);
      }

      BitmapIndexSeeker[] bitmapIndexSeeker = new BitmapIndexSeeker[indexes.size()];
      for (int j = 0; j < indexes.size(); j++) {
        bitmapIndexSeeker[j] = indexes.get(j).getBitmapIndexSeeker(dimension);
      }
      for (String dimVal : IndexedIterable.create(dimVals)) {
        progress.progress();
        List<Iterable<Integer>> convertedInverteds = Lists.newArrayListWithCapacity(indexes.size());
        for (int j = 0; j < indexes.size(); ++j) {
          convertedInverteds.add(
              new ConvertingIndexedInts(
                  bitmapIndexSeeker[j].seek(dimVal), rowNumConversions.get(j)
              )
          );
        }

        MutableBitmap bitset = bitmapSerdeFactory.getBitmapFactory().makeEmptyMutableBitmap();
        for (Integer row : CombiningIterable.createSplatted(
            convertedInverteds,
            Ordering.<Integer>natural().nullsFirst()
        )) {
          if (row != INVALID_ROW) {
            bitset.add(row);
          }
        }

        writer.write(
            bitmapSerdeFactory.getBitmapFactory().makeImmutableBitmap(bitset)
        );

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

    if (segmentMetadata != null && !segmentMetadata.isEmpty()) {
      writeMetadataToFile(new File(v8OutDir, "metadata.drd"), segmentMetadata);
      log.info("wrote metadata.drd in outDir[%s].", v8OutDir);

      expectedFiles.add("metadata.drd");
    }

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
        GenericIndexed.fromIterable(mergedDimensions, GenericIndexed.STRING_STRATEGY),
        GenericIndexed.fromIterable(mergedMetrics, GenericIndexed.STRING_STRATEGY),
        dataInterval,
        indexSpec.getBitmapSerdeFactory()
    );

    IndexIO.DefaultIndexIOHandler.convertV8toV9(v8OutDir, outDir, indexSpec);
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
      Interval dataInterval,
      BitmapSerdeFactory bitmapSerdeFactory
  ) throws IOException
  {
    File indexFile = new File(inDir, "index.drd");

    try (FileChannel channel = new FileOutputStream(indexFile).getChannel()) {
      channel.write(ByteBuffer.wrap(new byte[]{versionId}));

      availableDimensions.writeToChannel(channel);
      availableMetrics.writeToChannel(channel);
      serializerUtils.writeString(
          channel, String.format("%s/%s", dataInterval.getStart(), dataInterval.getEnd())
      );
      serializerUtils.writeString(
          channel, mapper.writeValueAsString(bitmapSerdeFactory)
      );
    }
    IndexIO.checkFileSize(indexFile);
  }

  private static class DimValueConverter
  {
    private final Indexed<String> dimSet;
    private final IntBuffer conversionBuf;

    private int currIndex;
    private String lastVal = null;
    private String currValue;

    DimValueConverter(
        Indexed<String> dimSet
    )
    {
      this.dimSet = dimSet;
      conversionBuf = ByteBuffer.allocateDirect(dimSet.size() * Ints.BYTES).asIntBuffer();

      currIndex = 0;
      currValue = null;
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
      if (currValue == null) {
        currValue = dimSet.get(currIndex);
      }

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
        } else {
          currValue = dimSet.get(currIndex);
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
                  input.getRowNum()
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
          lhs.getRowNum()
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

  static boolean isNullColumn(Iterable<String> dimValues)
  {
    if (dimValues == null) {
      return true;
    }
    for (String val : dimValues) {
      if (val != null) {
        return false;
      }
    }
    return true;
  }

  private static void writeMetadataToFile(File metadataFile, Map<String, Object> metadata) throws IOException
  {
    try (FileOutputStream metadataFileOutputStream = new FileOutputStream(metadataFile);
         FileChannel metadataFilechannel = metadataFileOutputStream.getChannel()
    ) {
      byte[] metadataBytes = mapper.writeValueAsBytes(metadata);
      if (metadataBytes.length != metadataFilechannel.write(ByteBuffer.wrap(metadataBytes))) {
        throw new IOException("Failed to write metadata for file");
      }
    }
    IndexIO.checkFileSize(metadataFile);
  }
}
