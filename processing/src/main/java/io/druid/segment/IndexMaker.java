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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
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
import com.metamx.common.io.smoosh.FileSmoosher;
import com.metamx.common.io.smoosh.SmooshedWriter;
import com.metamx.common.logger.Logger;
import io.druid.collections.CombiningIterable;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.SerializerUtils;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedFloatsIndexedSupplier;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressedVSizeIntsIndexedSupplier;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.VSizeIndexed;
import io.druid.segment.data.VSizeIndexedInts;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexAdapter;
import io.druid.segment.serde.ColumnPartSerde;
import io.druid.segment.serde.ComplexColumnPartSerde;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import io.druid.segment.serde.FloatGenericColumnPartSerde;
import io.druid.segment.serde.LongGenericColumnPartSerde;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@Deprecated
/**
 * This class is not yet ready for production use and requires more work. This class provides a demonstration of how
 * to build v9 segments directly.
 */
public class IndexMaker
{
  private static final Logger log = new Logger(IndexMaker.class);
  private static final SerializerUtils serializerUtils = new SerializerUtils();
  private static final int INVALID_ROW = -1;
  private static final Splitter SPLITTER = Splitter.on(",");
  private final ObjectMapper mapper;
  private final IndexIO indexIO;

  @Inject
  public IndexMaker(
      ObjectMapper mapper,
      IndexIO indexIO
  )
  {
    this.mapper = Preconditions.checkNotNull(mapper, "null ObjectMapper");
    this.indexIO = Preconditions.checkNotNull(indexIO, "null IndexIO");
  }

  public File persist(
      final IncrementalIndex index,
      File outDir,
      final Map<String, Object> segmentMetadata,
      final IndexSpec indexSpec
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
   * @throws java.io.IOException
   */
  public File persist(
      final IncrementalIndex index,
      final Interval dataInterval,
      File outDir,
      final Map<String, Object> segmentMetadata,
      final IndexSpec indexSpec
  ) throws IOException
  {
    return persist(
        index, dataInterval, outDir, segmentMetadata, indexSpec, new LoggingProgressIndicator(outDir.toString())
    );
  }

  public File persist(
      final IncrementalIndex index,
      final Interval dataInterval,
      File outDir,
      final Map<String, Object> segmentMetadata,
      final IndexSpec indexSpec,
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

  public File mergeQueryableIndex(
      List<QueryableIndex> indexes, final AggregatorFactory[] metricAggs, File outDir, final IndexSpec indexSpec
  ) throws IOException
  {
    return mergeQueryableIndex(indexes, metricAggs, outDir, indexSpec, new LoggingProgressIndicator(outDir.toString()));
  }

  public File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      final AggregatorFactory[] metricAggs,
      File outDir,
      final IndexSpec indexSpec,
      ProgressIndicator progress
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
        null,
        indexSpec,
        progress
    );
  }

  public File merge(
      List<IndexableAdapter> adapters, final AggregatorFactory[] metricAggs, File outDir, final IndexSpec indexSpec
  ) throws IOException
  {
    return merge(
        adapters, metricAggs, outDir, null, indexSpec, new LoggingProgressIndicator(outDir.toString())
    );
  }

  public File merge(
      List<IndexableAdapter> adapters,
      final AggregatorFactory[] metricAggs,
      File outDir,
      final Map<String, Object> segmentMetaData,
      final IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException
  {
    FileUtils.deleteDirectory(outDir);
    if (!outDir.mkdirs()) {
      throw new ISE("Couldn't make outdir[%s].", outDir);
    }

    final List<String> mergedDimensions = mergeIndexed(
        Lists.transform(
            adapters,
            new Function<IndexableAdapter, Iterable<String>>()
            {
              @Override
              public Iterable<String> apply(IndexableAdapter input)
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
                    .create(adapters)
                    .transform(
                        new Function<IndexableAdapter, Iterable<String>>()
                        {
                          @Override
                          public Iterable<String> apply(IndexableAdapter input)
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
          public String apply(String input)
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
          ArrayList<Iterable<Rowboat>> boats
      )
      {
        return CombiningIterable.create(
            new MergeIterable<>(
                Ordering.<Rowboat>natural().nullsFirst(),
                boats
            ),
            Ordering.<Rowboat>natural().nullsFirst(),
            new RowboatMergeFunction(sortedMetricAggs)
        );
      }
    };

    return makeIndexFiles(
        adapters, outDir, progress, mergedDimensions, mergedMetrics, segmentMetaData, rowMergerFn, indexSpec
    );
  }


  public File convert(final File inDir, final File outDir, final IndexSpec indexSpec) throws IOException
  {
    return convert(inDir, outDir, indexSpec, new BaseProgressIndicator());
  }

  public File convert(
      final File inDir, final File outDir, final IndexSpec indexSpec, final ProgressIndicator progress
  ) throws IOException
  {
    try (QueryableIndex index = indexIO.loadIndex(inDir)) {
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

  public File append(
      final List<IndexableAdapter> adapters,
      final File outDir,
      final IndexSpec indexSpec
  ) throws IOException
  {
    return append(adapters, outDir, new LoggingProgressIndicator(outDir.toString()), indexSpec);
  }

  public File append(
      final List<IndexableAdapter> adapters,
      final File outDir,
      final ProgressIndicator progress,
      final IndexSpec indexSpec
  ) throws IOException
  {
    FileUtils.deleteDirectory(outDir);
    if (!outDir.mkdirs()) {
      throw new ISE("Couldn't make outdir[%s].", outDir);
    }

    final List<String> mergedDimensions = mergeIndexed(
        Lists.transform(
            adapters,
            new Function<IndexableAdapter, Iterable<String>>()
            {
              @Override
              public Iterable<String> apply(IndexableAdapter input)
              {
                return Iterables.transform(
                    input.getDimensionNames(),
                    new Function<String, String>()
                    {
                      @Override
                      public String apply(String input)
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
            adapters,
            new Function<IndexableAdapter, Iterable<String>>()
            {
              @Override
              public Iterable<String> apply(IndexableAdapter input)
              {
                return Iterables.transform(
                    input.getMetricNames(),
                    new Function<String, String>()
                    {
                      @Override
                      public String apply(String input)
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
          final ArrayList<Iterable<Rowboat>> boats
      )
      {
        return new MergeIterable<>(
            Ordering.<Rowboat>natural().nullsFirst(),
            boats
        );
      }
    };

    return makeIndexFiles(adapters, outDir, progress, mergedDimensions, mergedMetrics, null, rowMergerFn, indexSpec);
  }

  private File makeIndexFiles(
      final List<IndexableAdapter> adapters,
      final File outDir,
      final ProgressIndicator progress,
      final List<String> mergedDimensions,
      final List<String> mergedMetrics,
      final Map<String, Object> segmentMetadata,
      final Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>> rowMergerFn,
      final IndexSpec indexSpec
  ) throws IOException
  {
    progress.start();
    progress.progress();

    final Map<String, ValueType> valueTypes = Maps.newTreeMap(Ordering.<String>natural().nullsFirst());
    final Map<String, String> metricTypeNames = Maps.newTreeMap(Ordering.<String>natural().nullsFirst());
    final Map<String, ColumnCapabilitiesImpl> columnCapabilities = Maps.newHashMap();

    for (IndexableAdapter adapter : adapters) {
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

    outDir.mkdirs();
    final FileSmoosher v9Smoosher = new FileSmoosher(outDir);

    ByteStreams.write(
        Ints.toByteArray(IndexIO.V9_VERSION),
        Files.newOutputStreamSupplier(new File(outDir, "version.bin"))
    );

    final Map<String, Integer> dimIndexes = Maps.newHashMap();
    final Map<String, Iterable<String>> dimensionValuesLookup = Maps.newHashMap();
    final ArrayList<Map<String, IntBuffer>> dimConversions = Lists.newArrayListWithCapacity(adapters.size());
    final Set<String> skippedDimensions = Sets.newHashSet();
    final List<IntBuffer> rowNumConversions = Lists.newArrayListWithCapacity(adapters.size());

    progress.progress();
    setupDimConversion(
        adapters,
        progress,
        mergedDimensions,
        dimConversions,
        dimIndexes,
        skippedDimensions,
        dimensionValuesLookup
    );

    progress.progress();
    final Iterable<Rowboat> theRows = makeRowIterable(
        adapters,
        mergedDimensions,
        mergedMetrics,
        dimConversions,
        rowMergerFn
    );

    progress.progress();
    final int rowCount = convertDims(adapters, progress, theRows, rowNumConversions);

    progress.progress();
    makeTimeColumn(v9Smoosher, progress, theRows, rowCount);

    progress.progress();
    makeDimColumns(
        v9Smoosher,
        adapters,
        progress,
        mergedDimensions,
        skippedDimensions,
        theRows,
        columnCapabilities,
        dimensionValuesLookup,
        rowNumConversions,
        indexSpec
    );

    progress.progress();
    makeMetricColumns(v9Smoosher, progress, theRows, mergedMetrics, valueTypes, metricTypeNames, rowCount, indexSpec);

    progress.progress();
    makeIndexBinary(
        v9Smoosher, adapters, outDir, mergedDimensions, mergedMetrics, skippedDimensions, progress, indexSpec
    );
    makeMetadataBinary(v9Smoosher, progress, segmentMetadata);

    v9Smoosher.close();

    progress.stop();

    return outDir;
  }

  private void setupDimConversion(
      final List<IndexableAdapter> adapters,
      final ProgressIndicator progress,
      final List<String> mergedDimensions,
      final List<Map<String, IntBuffer>> dimConversions,
      final Map<String, Integer> dimIndexes,
      final Set<String> skippedDimensions,
      final Map<String, Iterable<String>> dimensionValuesLookup
  )
  {
    final String section = "setup dimension conversions";
    progress.startSection(section);

    for (IndexableAdapter adapter : adapters) {
      dimConversions.add(Maps.<String, IntBuffer>newHashMap());
    }

    int dimIndex = 0;
    for (String dimension : mergedDimensions) {
      dimIndexes.put(dimension, dimIndex++);

      // lookups for all dimension values of this dimension
      final List<Indexed<String>> dimValueLookups = Lists.newArrayListWithCapacity(adapters.size());

      // each converter converts dim values of this dimension to global dictionary
      final DimValueConverter[] converters = new DimValueConverter[adapters.size()];

      for (int i = 0; i < adapters.size(); i++) {
        Indexed<String> dimValues = adapters.get(i).getDimValueLookup(dimension);
        if (!IndexMerger.isNullColumn(dimValues)) {
          dimValueLookups.add(dimValues);
          converters[i] = new DimValueConverter(dimValues);
        }
      }

      // sort all dimension values and treat all null values as empty strings
      final Iterable<String> dimensionValues = CombiningIterable.createSplatted(
          Iterables.transform(
              dimValueLookups,
              new Function<Indexed<String>, Iterable<String>>()
              {
                @Override
                public Iterable<String> apply(Indexed<String> indexed)
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
          ),
          Ordering.<String>natural()
      );

      int cardinality = 0;
      for (String value : dimensionValues) {
        for (int i = 0; i < adapters.size(); i++) {
          DimValueConverter converter = converters[i];
          if (converter != null) {
            converter.convert(value, cardinality);
          }
        }

        ++cardinality;
      }

      if (cardinality == 0) {
        log.info("Skipping [%s], it is empty!", dimension);
        skippedDimensions.add(dimension);
        continue;
      }

      dimensionValuesLookup.put(dimension, dimensionValues);

      // make the dictionary
      for (int i = 0; i < adapters.size(); ++i) {
        DimValueConverter converter = converters[i];
        if (converter != null) {
          dimConversions.get(i).put(dimension, converters[i].getConversionBuffer());
        }
      }
    }

    progress.stopSection(section);
  }

  private Iterable<Rowboat> makeRowIterable(
      final List<IndexableAdapter> adapters,
      final List<String> mergedDimensions,
      final List<String> mergedMetrics,
      final ArrayList<Map<String, IntBuffer>> dimConversions,
      final Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>> rowMergerFn
  )
  {
    ArrayList<Iterable<Rowboat>> boats = Lists.newArrayListWithCapacity(adapters.size());

    for (int i = 0; i < adapters.size(); ++i) {
      final IndexableAdapter adapter = adapters.get(i);

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
                  adapters.get(i).getRows(),
                  new Function<Rowboat, Rowboat>()
                  {
                    @Override
                    public Rowboat apply(Rowboat input)
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
              ),
              mergedDimensions,
              dimConversions.get(i),
              i
          )
      );
    }

    return rowMergerFn.apply(boats);
  }

  private int convertDims(
      final List<IndexableAdapter> adapters,
      final ProgressIndicator progress,
      final Iterable<Rowboat> theRows,
      final List<IntBuffer> rowNumConversions
  ) throws IOException
  {
    final String section = "convert dims";
    progress.startSection(section);

    for (IndexableAdapter index : adapters) {
      int[] arr = new int[index.getNumRows()];
      Arrays.fill(arr, INVALID_ROW);
      rowNumConversions.add(IntBuffer.wrap(arr));
    }

    int rowCount = 0;
    for (Rowboat theRow : theRows) {
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
        progress.progressSection(section, String.format("Walked 500,000/%,d rows", rowCount));
      }
    }

    for (IntBuffer rowNumConversion : rowNumConversions) {
      rowNumConversion.rewind();
    }

    progress.stopSection(section);

    return rowCount;
  }

  private void makeTimeColumn(
      final FileSmoosher v9Smoosher,
      final ProgressIndicator progress,
      final Iterable<Rowboat> theRows,
      final int rowCount
  ) throws IOException
  {
    final String section = "make time column";
    progress.startSection(section);

    long[] longs = new long[rowCount];

    int rowNum = 0;
    for (Rowboat theRow : theRows) {
      longs[rowNum++] = theRow.getTimestamp();
    }

    CompressedLongsIndexedSupplier timestamps = CompressedLongsIndexedSupplier.fromLongBuffer(
        LongBuffer.wrap(longs),
        IndexIO.BYTE_ORDER,
        CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY

    );

    final ColumnDescriptor.Builder timeBuilder = ColumnDescriptor.builder();
    timeBuilder.setValueType(ValueType.LONG);

    writeColumn(
        v9Smoosher,
        new LongGenericColumnPartSerde(timestamps, IndexIO.BYTE_ORDER),
        timeBuilder,
        "__time"
    );

    progress.stopSection(section);
  }

  private void makeDimColumns(
      final FileSmoosher v9Smoosher,
      final List<IndexableAdapter> adapters,
      final ProgressIndicator progress,
      final List<String> mergedDimensions,
      final Set<String> skippedDimensions,
      final Iterable<Rowboat> theRows,
      final Map<String, ColumnCapabilitiesImpl> columnCapabilities,
      final Map<String, Iterable<String>> dimensionValuesLookup,
      final List<IntBuffer> rowNumConversions,
      final IndexSpec indexSpec
  ) throws IOException
  {
    final String dimSection = "make dimension columns";
    progress.startSection(dimSection);

    int dimIndex = 0;
    for (String dimension : mergedDimensions) {
      if (skippedDimensions.contains(dimension)) {
        dimIndex++;
        continue;
      }

      makeDimColumn(
          v9Smoosher,
          adapters,
          progress,
          theRows,
          dimIndex,
          dimension,
          columnCapabilities,
          dimensionValuesLookup,
          rowNumConversions,
          indexSpec.getBitmapSerdeFactory(),
          indexSpec.getDimensionCompressionStrategy()
      );
      dimIndex++;
    }
    progress.stopSection(dimSection);
  }

  private static class NullsAtZeroConvertingIntList extends AbstractList<Integer>
  {
    private final List<Integer> delegate;
    private final boolean delegateHasNullAtZero;

    NullsAtZeroConvertingIntList(List<Integer> delegate, final boolean delegateHasNullAtZero)
    {
      this.delegate = delegate;
      this.delegateHasNullAtZero = delegateHasNullAtZero;
    }

    @Override
    public Integer get(int index)
    {
      Integer val = delegate.get(index);
      if (val == null) {
        return 0;
      }
      return delegateHasNullAtZero ? val : val + 1;
    }

    @Override
    public int size()
    {
      return delegate.size();
    }
  }

  private void makeDimColumn(
      final FileSmoosher v9Smoosher,
      final List<IndexableAdapter> adapters,
      final ProgressIndicator progress,
      final Iterable<Rowboat> theRows,
      final int dimIndex,
      final String dimension,
      final Map<String, ColumnCapabilitiesImpl> columnCapabilities,
      final Map<String, Iterable<String>> dimensionValuesLookup,
      final List<IntBuffer> rowNumConversions,
      final BitmapSerdeFactory bitmapSerdeFactory,
      final CompressedObjectStrategy.CompressionStrategy compressionStrategy
  ) throws IOException
  {
    final String section = String.format("make %s", dimension);
    progress.startSection(section);

    final ColumnDescriptor.Builder dimBuilder = ColumnDescriptor.builder();
    dimBuilder.setValueType(ValueType.STRING);

    final List<ByteBuffer> outParts = Lists.newArrayList();

    ByteArrayOutputStream nameBAOS = new ByteArrayOutputStream();
    serializerUtils.writeString(nameBAOS, dimension);
    outParts.add(ByteBuffer.wrap(nameBAOS.toByteArray()));

    boolean hasMultipleValues = columnCapabilities.get(dimension).hasMultipleValues();
    dimBuilder.setHasMultipleValues(hasMultipleValues);

    // make dimension columns
    List<Integer> singleValCol;
    final VSizeIndexed multiValCol;

    ColumnDictionaryEntryStore adder = hasMultipleValues
                                       ? new MultiValColumnDictionaryEntryStore()
                                       : new SingleValColumnDictionaryEntryStore();

    final BitmapFactory bitmapFactory = bitmapSerdeFactory.getBitmapFactory();
    MutableBitmap nullSet = null;
    int rowCount = 0;

    for (Rowboat theRow : theRows) {
      if (dimIndex > theRow.getDims().length) {
        if (nullSet == null) {
          nullSet = bitmapFactory.makeEmptyMutableBitmap();
        }
        nullSet.add(rowCount);
        adder.add(null);
      } else {
        int[] dimVals = theRow.getDims()[dimIndex];
        if (dimVals == null || dimVals.length == 0) {
          if (nullSet == null) {
            nullSet = bitmapFactory.makeEmptyMutableBitmap();
          }
          nullSet.add(rowCount);
        }
        adder.add(dimVals);
      }
      rowCount++;
    }

    final Iterable<String> dimensionValues = dimensionValuesLookup.get(dimension);
    GenericIndexed<String> dictionary = GenericIndexed.fromIterable(
        dimensionValues,
        GenericIndexed.STRING_STRATEGY
    );
    boolean bumpDictionary = false;

    if (hasMultipleValues) {
      final List<List<Integer>> vals = ((MultiValColumnDictionaryEntryStore) adder).get();
      if (nullSet != null) {
        log.info("Dimension[%s] has null rows.", dimension);

        if (Iterables.getFirst(dimensionValues, "") != null) {
          bumpDictionary = true;
          log.info("Dimension[%s] has no null value in the dictionary, expanding...", dimension);

          dictionary = GenericIndexed.fromIterable(
              Iterables.concat(Collections.<String>singleton(null), dimensionValues),
              GenericIndexed.STRING_STRATEGY
          );

          final int dictionarySize = dictionary.size();

          singleValCol = null;
          multiValCol = VSizeIndexed.fromIterable(
              Iterables.transform(
                  vals,
                  new Function<List<Integer>, VSizeIndexedInts>()
                  {
                    @Override
                    public VSizeIndexedInts apply(final List<Integer> input)
                    {
                      if (input == null) {
                        return VSizeIndexedInts.fromList(ImmutableList.<Integer>of(0), dictionarySize);
                      } else {
                        return VSizeIndexedInts.fromList(
                            new NullsAtZeroConvertingIntList(input, false),
                            dictionarySize
                        );
                      }
                    }
                  }
              )
          );
        } else {
          final int dictionarySize = dictionary.size();
          singleValCol = null;
          multiValCol = VSizeIndexed.fromIterable(
              Iterables.transform(
                  vals,
                  new Function<List<Integer>, VSizeIndexedInts>()
                  {
                    @Override
                    public VSizeIndexedInts apply(List<Integer> input)
                    {
                      if (input == null) {
                        return VSizeIndexedInts.fromList(ImmutableList.<Integer>of(0), dictionarySize);
                      } else {
                        return VSizeIndexedInts.fromList(input, dictionarySize);
                      }
                    }
                  }
              )
          );
        }
      } else {
        final int dictionarySize = dictionary.size();
        singleValCol = null;
        multiValCol = VSizeIndexed.fromIterable(
            Iterables.transform(
                vals,
                new Function<List<Integer>, VSizeIndexedInts>()
                {
                  @Override
                  public VSizeIndexedInts apply(List<Integer> input)
                  {
                    return VSizeIndexedInts.fromList(input, dictionarySize);
                  }
                }
            )
        );
      }
    } else {
      final List<Integer> vals = ((SingleValColumnDictionaryEntryStore) adder).get();

      if (nullSet != null) {
        log.info("Dimension[%s] has null rows.", dimension);

        if (Iterables.getFirst(dimensionValues, "") != null) {
          bumpDictionary = true;
          log.info("Dimension[%s] has no null value in the dictionary, expanding...", dimension);

          final List<String> nullList = Lists.newArrayList();
          nullList.add(null);

          dictionary = GenericIndexed.fromIterable(
              Iterables.concat(nullList, dimensionValues),
              GenericIndexed.STRING_STRATEGY
          );
          multiValCol = null;
          singleValCol = new NullsAtZeroConvertingIntList(vals, false);
        } else {
          multiValCol = null;
          singleValCol = new NullsAtZeroConvertingIntList(vals, true);
        }
      } else {
        multiValCol = null;
        singleValCol = new AbstractList<Integer>()
        {
          @Override
          public Integer get(int index)
          {
            return vals.get(index);
          }

          @Override
          public int size()
          {
            return vals.size();
          }
        };
      }
    }

    // Make bitmap indexes
    List<MutableBitmap> mutableBitmaps = Lists.newArrayList();
    for (String dimVal : dimensionValues) {
      List<Iterable<Integer>> convertedInverteds = Lists.newArrayListWithCapacity(adapters.size());
      for (int j = 0; j < adapters.size(); ++j) {
        convertedInverteds.add(
            new ConvertingIndexedInts(
                adapters.get(j).getBitmapIndex(dimension, dimVal), rowNumConversions.get(j)
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

      mutableBitmaps.add(bitset);
    }

    GenericIndexed<ImmutableBitmap> bitmaps;

    if (nullSet != null) {
      final ImmutableBitmap theNullSet = bitmapFactory.makeImmutableBitmap(nullSet);
      if (bumpDictionary) {
        bitmaps = GenericIndexed.fromIterable(
            Iterables.concat(
                Arrays.asList(theNullSet),
                Iterables.transform(
                    mutableBitmaps,
                    new Function<MutableBitmap, ImmutableBitmap>()
                    {
                      @Override
                      public ImmutableBitmap apply(MutableBitmap input)
                      {
                        return bitmapFactory.makeImmutableBitmap(input);
                      }
                    }
                )
            ),
            bitmapSerdeFactory.getObjectStrategy()
        );
      } else {
        Iterable<ImmutableBitmap> immutableBitmaps = Iterables.transform(
            mutableBitmaps,
            new Function<MutableBitmap, ImmutableBitmap>()
            {
              @Override
              public ImmutableBitmap apply(MutableBitmap input)
              {
                return bitmapFactory.makeImmutableBitmap(input);
              }
            }
        );

        bitmaps = GenericIndexed.fromIterable(
            Iterables.concat(
                Arrays.asList(
                    theNullSet.union(Iterables.getFirst(immutableBitmaps, null))
                ),
                Iterables.skip(immutableBitmaps, 1)
            ),
            bitmapSerdeFactory.getObjectStrategy()
        );
      }
    } else {
      bitmaps = GenericIndexed.fromIterable(
          Iterables.transform(
              mutableBitmaps,
              new Function<MutableBitmap, ImmutableBitmap>()
              {
                @Override
                public ImmutableBitmap apply(MutableBitmap input)
                {
                  return bitmapFactory.makeImmutableBitmap(input);
                }
              }
          ),
          bitmapSerdeFactory.getObjectStrategy()
      );
    }

    // Make spatial indexes
    ImmutableRTree spatialIndex = null;
    boolean hasSpatialIndexes = columnCapabilities.get(dimension).hasSpatialIndexes();
    RTree tree = null;
    if (hasSpatialIndexes) {
      tree = new RTree(
          2,
          new LinearGutmanSplitStrategy(0, 50, bitmapSerdeFactory.getBitmapFactory()),
          bitmapSerdeFactory.getBitmapFactory()
      );
    }

    int dimValIndex = 0;
    for (String dimVal : dimensionValuesLookup.get(dimension)) {
      if (hasSpatialIndexes) {
        if (dimVal != null && !dimVal.isEmpty()) {
          List<String> stringCoords = Lists.newArrayList(SPLITTER.split(dimVal));
          float[] coords = new float[stringCoords.size()];
          for (int j = 0; j < coords.length; j++) {
            coords[j] = Float.valueOf(stringCoords.get(j));
          }
          tree.insert(coords, mutableBitmaps.get(dimValIndex));
        }
        dimValIndex++;
      }
    }
    if (hasSpatialIndexes) {
      spatialIndex = ImmutableRTree.newImmutableFromMutable(tree);
    }

    log.info("Completed dimension[%s] with cardinality[%,d]. Starting write.", dimension, dictionary.size());

    final DictionaryEncodedColumnPartSerde.Builder dimPartBuilder = DictionaryEncodedColumnPartSerde
        .builder()
        .withDictionary(dictionary)
        .withBitmapSerdeFactory(bitmapSerdeFactory)
        .withBitmaps(bitmaps)
        .withSpatialIndex(spatialIndex)
        .withByteOrder(IndexIO.BYTE_ORDER);

    if (singleValCol != null) {
      if (compressionStrategy != null) {
        dimPartBuilder.withSingleValuedColumn(
            CompressedVSizeIntsIndexedSupplier.fromList(
                singleValCol,
                dictionary.size(),
                CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForValue(dictionary.size()),
                IndexIO.BYTE_ORDER,
                compressionStrategy
            )
        );
      } else {
        dimPartBuilder.withSingleValuedColumn(VSizeIndexedInts.fromList(singleValCol, dictionary.size()));
      }
    } else if (compressionStrategy != null) {
      dimPartBuilder.withMultiValuedColumn(
          CompressedVSizeIndexedSupplier.fromIterable(
              multiValCol,
              dictionary.size(),
              IndexIO.BYTE_ORDER,
              compressionStrategy
          )
      );
    } else {
      dimPartBuilder.withMultiValuedColumn(multiValCol);
    }


    writeColumn(
        v9Smoosher,
        dimPartBuilder.build(),
        dimBuilder,
        dimension
    );

    progress.stopSection(section);
  }

  private void makeMetricColumns(
      final FileSmoosher v9Smoosher,
      final ProgressIndicator progress,
      final Iterable<Rowboat> theRows,
      final List<String> mergedMetrics,
      final Map<String, ValueType> valueTypes,
      final Map<String, String> metricTypeNames,
      final int rowCount,
      final IndexSpec indexSpec
  ) throws IOException
  {
    final String metSection = "make metric columns";
    progress.startSection(metSection);

    int metIndex = 0;
    for (String metric : mergedMetrics) {
      makeMetricColumn(
          v9Smoosher,
          progress,
          theRows,
          metIndex,
          metric,
          valueTypes,
          metricTypeNames,
          rowCount,
          indexSpec.getMetricCompressionStrategy()
      );
      metIndex++;
    }
    progress.stopSection(metSection);
  }

  private void makeMetricColumn(
      final FileSmoosher v9Smoosher,
      final ProgressIndicator progress,
      final Iterable<Rowboat> theRows,
      final int metricIndex,
      final String metric,
      final Map<String, ValueType> valueTypes,
      final Map<String, String> metricTypeNames,
      final int rowCount,
      final CompressedObjectStrategy.CompressionStrategy compressionStrategy
  ) throws IOException
  {
    final String section = String.format("make column[%s]", metric);
    progress.startSection(section);

    final ColumnDescriptor.Builder metBuilder = ColumnDescriptor.builder();
    ValueType type = valueTypes.get(metric);

    switch (type) {
      case FLOAT: {
        metBuilder.setValueType(ValueType.FLOAT);

        float[] arr = new float[rowCount];
        int rowNum = 0;
        for (Rowboat theRow : theRows) {
          Object obj = theRow.getMetrics()[metricIndex];
          arr[rowNum++] = (obj == null) ? 0 : ((Number) obj).floatValue();
        }

        CompressedFloatsIndexedSupplier compressedFloats = CompressedFloatsIndexedSupplier.fromFloatBuffer(
            FloatBuffer.wrap(arr),
            IndexIO.BYTE_ORDER,
            compressionStrategy
        );

        writeColumn(
            v9Smoosher,
            new FloatGenericColumnPartSerde(compressedFloats, IndexIO.BYTE_ORDER),
            metBuilder,
            metric
        );
        break;
      }
      case LONG: {
        metBuilder.setValueType(ValueType.LONG);

        long[] arr = new long[rowCount];
        int rowNum = 0;
        for (Rowboat theRow : theRows) {
          Object obj = theRow.getMetrics()[metricIndex];
          arr[rowNum++] = (obj == null) ? 0 : ((Number) obj).longValue();
        }

        CompressedLongsIndexedSupplier compressedLongs = CompressedLongsIndexedSupplier.fromLongBuffer(
            LongBuffer.wrap(arr),
            IndexIO.BYTE_ORDER,
            compressionStrategy
        );

        writeColumn(
            v9Smoosher,
            new LongGenericColumnPartSerde(compressedLongs, IndexIO.BYTE_ORDER),
            metBuilder,
            metric
        );
        break;
      }
      case COMPLEX:
        String complexType = metricTypeNames.get(metric);

        ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(complexType);

        if (serde == null) {
          throw new ISE("Unknown type[%s]", complexType);
        }

        final GenericIndexed metricColumn = GenericIndexed.fromIterable(
            Iterables.transform(
                theRows,
                new Function<Rowboat, Object>()
                {
                  @Override
                  public Object apply(Rowboat input)
                  {
                    return input.getMetrics()[metricIndex];
                  }
                }
            ),
            serde.getObjectStrategy()
        );

        metBuilder.setValueType(ValueType.COMPLEX);
        writeColumn(
            v9Smoosher,
            new ComplexColumnPartSerde(metricColumn, complexType),
            metBuilder,
            metric
        );
        break;
      default:
        throw new ISE("Unknown type[%s]", type);
    }

    progress.stopSection(section);
  }

  private void makeIndexBinary(
      final FileSmoosher v9Smoosher,
      final List<IndexableAdapter> adapters,
      final File outDir,
      final List<String> mergedDimensions,
      final List<String> mergedMetrics,
      final Set<String> skippedDimensions,
      final ProgressIndicator progress,
      final IndexSpec indexSpec
  ) throws IOException
  {
    final String section = "building index.drd";
    progress.startSection(section);

    final Set<String> finalColumns = Sets.newTreeSet();
    finalColumns.addAll(mergedDimensions);
    finalColumns.addAll(mergedMetrics);
    finalColumns.removeAll(skippedDimensions);

    final Iterable<String> finalDimensions = Iterables.filter(
        mergedDimensions,
        new Predicate<String>()
        {
          @Override
          public boolean apply(String input)
          {
            return !skippedDimensions.contains(input);
          }
        }
    );

    GenericIndexed<String> cols = GenericIndexed.fromIterable(finalColumns, GenericIndexed.STRING_STRATEGY);
    GenericIndexed<String> dims = GenericIndexed.fromIterable(finalDimensions, GenericIndexed.STRING_STRATEGY);

    final String bitmapSerdeFactoryType = mapper.writeValueAsString(indexSpec.getBitmapSerdeFactory());
    final long numBytes = cols.getSerializedSize()
                          + dims.getSerializedSize()
                          + 16
                          + serializerUtils.getSerializedStringByteSize(bitmapSerdeFactoryType);

    final SmooshedWriter writer = v9Smoosher.addWithSmooshedWriter("index.drd", numBytes);
    cols.writeToChannel(writer);
    dims.writeToChannel(writer);

    DateTime minTime = new DateTime(JodaUtils.MAX_INSTANT);
    DateTime maxTime = new DateTime(JodaUtils.MIN_INSTANT);

    for (IndexableAdapter index : adapters) {
      minTime = JodaUtils.minDateTime(minTime, index.getDataInterval().getStart());
      maxTime = JodaUtils.maxDateTime(maxTime, index.getDataInterval().getEnd());
    }
    final Interval dataInterval = new Interval(minTime, maxTime);

    serializerUtils.writeLong(writer, dataInterval.getStartMillis());
    serializerUtils.writeLong(writer, dataInterval.getEndMillis());

    serializerUtils.writeString(
        writer, bitmapSerdeFactoryType
    );
    writer.close();

    IndexIO.checkFileSize(new File(outDir, "index.drd"));

    progress.stopSection(section);
  }

  private void makeMetadataBinary(
      final FileSmoosher v9Smoosher,
      final ProgressIndicator progress,
      final Map<String, Object> segmentMetadata
  ) throws IOException
  {
    if (segmentMetadata != null && !segmentMetadata.isEmpty()) {
      progress.startSection("metadata.drd");
      v9Smoosher.add("metadata.drd", ByteBuffer.wrap(mapper.writeValueAsBytes(segmentMetadata)));
      progress.stopSection("metadata.drd");
    }
  }

  private void writeColumn(
      FileSmoosher v9Smoosher,
      ColumnPartSerde serde,
      ColumnDescriptor.Builder builder,
      String name
  ) throws IOException
  {
    builder.addSerde(serde);

    final ColumnDescriptor descriptor = builder.build();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serializerUtils.writeString(baos, mapper.writeValueAsString(descriptor));
    byte[] specBytes = baos.toByteArray();

    final SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter(
        name, descriptor.numBytes() + specBytes.length
    );
    channel.write(ByteBuffer.wrap(specBytes));
    descriptor.write(channel);
    channel.close();
  }

  private <T extends Comparable> ArrayList<T> mergeIndexed(final List<Iterable<T>> indexedLists)
  {
    Set<T> retVal = Sets.newTreeSet(Ordering.<T>natural().nullsFirst());

    for (Iterable<T> indexedList : indexedLists) {
      for (T val : indexedList) {
        retVal.add(val);
      }
    }

    return Lists.newArrayList(retVal);
  }

  private static interface ColumnDictionaryEntryStore
  {
    public void add(int[] vals);
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
      final int bufferSize = dimSet.size() * Ints.BYTES;
      log.info("Allocating new dimension conversion buffer of size[%,d]", bufferSize);
      this.conversionBuf = ByteBuffer.allocateDirect(bufferSize).asIntBuffer();

      this.currIndex = 0;
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
            public Integer apply(Integer input)
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
      final IntBuffer[] converterArray = FluentIterable
          .from(convertedDims)
          .transform(
              new Function<String, IntBuffer>()
              {
                @Override
                public IntBuffer apply(String input)
                {
                  return converters.get(input);
                }
              }
          ).toArray(IntBuffer.class);
      return Iterators.transform(
          index.iterator(),
          new Function<Rowboat, Rowboat>()
          {
            @Override
            public Rowboat apply(Rowboat input)
            {
              final int[][] dims = input.getDims();
              final int[][] newDims = new int[convertedDims.size()][];
              for (int i = 0; i < newDims.length; ++i) {
                final IntBuffer converter = converterArray[i];

                if (converter == null) {
                  continue;
                }

                if (i >= dims.length || dims[i] == null) {
                  continue;
                }

                newDims[i] = new int[dims[i].length];

                for (int j = 0; j < dims[i].length; ++j) {
                  if (!converter.hasRemaining()) {
                    throw new ISE("Converter mismatch! wtfbbq!");
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

  private static class SingleValColumnDictionaryEntryStore implements ColumnDictionaryEntryStore
  {
    private final List<Integer> data = Lists.newArrayList();

    @Override
    public void add(int[] vals)
    {
      if (vals == null || vals.length == 0) {
        data.add(null);
      } else {
        data.add(vals[0]);
      }
    }

    public List<Integer> get()
    {
      return data;
    }
  }

  private static class MultiValColumnDictionaryEntryStore implements ColumnDictionaryEntryStore
  {
    private final List<List<Integer>> data = Lists.newArrayList();

    public void add(int[] vals)
    {
      if (vals == null || vals.length == 0) {
        data.add(null);
      } else {
        data.add(Ints.asList(vals));
      }
    }

    public List<List<Integer>> get()
    {
      return data;
    }
  }
}
