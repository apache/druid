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
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
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
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.ToLowerCaseAggregatorFactory;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.CompressedFloatsIndexedSupplier;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.ConciseCompressedIndexedInts;
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
import it.uniroma3.mat.extendedset.intset.ConciseSet;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
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

/**
 */
public class IndexMaker
{
  private static final Logger log = new Logger(IndexMaker.class);

  private static final SerializerUtils serializerUtils = new SerializerUtils();
  private static final int INVALID_ROW = -1;
  private static final Splitter SPLITTER = Splitter.on(",");
  // This should really be provided by DI, should be changed once we switch around to using a DI framework
  private static final ObjectMapper mapper = new DefaultObjectMapper();


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
  public static File persist(
      final IncrementalIndex index,
      final Interval dataInterval,
      File outDir
  ) throws IOException
  {
    return persist(index, dataInterval, outDir, new NoopProgressIndicator());
  }

  public static File persist(
      final IncrementalIndex index,
      final Interval dataInterval,
      File outDir,
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
      List<QueryableIndex> indexes,
      final AggregatorFactory[] metricAggs,
      File outDir,
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
        progress
    );
  }

  public static File merge(
      List<IndexableAdapter> adapters, final AggregatorFactory[] metricAggs, File outDir
  ) throws IOException
  {
    return merge(adapters, metricAggs, outDir, new NoopProgressIndicator());
  }

  public static File merge(
      List<IndexableAdapter> adapters,
      final AggregatorFactory[] metricAggs,
      File outDir,
      ProgressIndicator progress
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
            Lists.newArrayList(
                FunctionalIterable
                    .create(adapters)
                    .transform(
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
          public String apply(String input)
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

    return makeIndexFiles(mapper, adapters, outDir, progress, mergedDimensions, mergedMetrics, rowMergerFn);
  }

  public static File append(
      final List<IndexableAdapter> adapters,
      File outDir
  ) throws IOException
  {
    return append(adapters, outDir, new NoopProgressIndicator());
  }

  public static File append(
      final List<IndexableAdapter> adapters,
      final File outDir,
      final ProgressIndicator progress
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
          final ArrayList<Iterable<Rowboat>> boats
      )
      {
        return new MergeIterable<>(
            Ordering.<Rowboat>natural().nullsFirst(),
            boats
        );
      }
    };

    return makeIndexFiles(mapper, adapters, outDir, progress, mergedDimensions, mergedMetrics, rowMergerFn);
  }

  private static File makeIndexFiles(
      final ObjectMapper mapper,
      final List<IndexableAdapter> adapters,
      final File outDir,
      final ProgressIndicator progress,
      final List<String> mergedDimensions,
      final List<String> mergedMetrics,
      final Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>> rowMergerFn
  ) throws IOException
  {
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

    long startTime = System.currentTimeMillis();

    /************* Setup Dim Conversions **************/
    progress.progress();
    startTime = System.currentTimeMillis();

    final Map<String, Integer> dimIndexes = Maps.newHashMap();
    final Map<String, Iterable<String>> dimensionValuesLookup = Maps.newHashMap();
    final ArrayList<Map<String, IntBuffer>> dimConversions = Lists.newArrayListWithCapacity(adapters.size());
    final Set<String> skippedDimensions = Sets.newHashSet();

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
        if (dimValues != null) {
          dimValueLookups.add(dimValues);
          converters[i] = new DimValueConverter(dimValues);
        }
      }

      // sort all dimension values and treat all null values as empty strings
      final Iterable<String> dimensionValues = CombiningIterable.createSplatted(
          dimValueLookups,
          //Iterables.transform(
          //    dimValueLookups,
          //    new Function<Indexed<String>, Iterable<String>>()
          //    {
          //      @Override
          //      public Iterable<String> apply(Indexed<String> indexed)
          //      {
          //        return Iterables.transform(
          //            indexed,
          //            new Function<String, String>()
          //            {
          //              @Override
          //              public String apply(String input)
          //              {
          //                return (input == null) ? "" : input;
          //              }
          //            }
          //        );
          //      }
          //    }
          //),
          Ordering.<String>natural().nullsFirst()
      );

      int cardinality = 0;
      for (String value : dimensionValues) {
        //if (value != null) {
        for (int i = 0; i < adapters.size(); i++) {
          DimValueConverter converter = converters[i];
          if (converter != null) {
            converter.convert(value, cardinality);
          }
        }
        //}

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

    final Iterable<Rowboat> theRows = makeRowIterable(
        adapters,
        mergedDimensions,
        mergedMetrics,
        dimConversions,
        rowMergerFn
    );

    /************* Do Dim Conversions **************/
    List<IntBuffer> rowNumConversions = Lists.newArrayListWithCapacity(adapters.size());
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
        log.info(
            "outDir[%s] walked 500,000/%,d rows in %,d millis.",
            outDir,
            rowCount,
            System.currentTimeMillis() - startTime
        );
      }
    }

    for (IntBuffer rowNumConversion : rowNumConversions) {
      rowNumConversion.rewind();
    }

    /************* Write time column **************/
    long[] longs = new long[rowCount];

    int rowNum = 0;
    for (Rowboat theRow : theRows) {
      longs[rowNum++] = theRow.getTimestamp();
    }

    CompressedLongsIndexedSupplier timestamps = CompressedLongsIndexedSupplier.fromLongBuffer(
        LongBuffer.wrap(longs),
        IndexIO.BYTE_ORDER
    );

    final ColumnDescriptor.Builder timeBuilder = ColumnDescriptor.builder();
    timeBuilder.setValueType(ValueType.LONG);

    writeColumn(
        mapper,
        v9Smoosher,
        new LongGenericColumnPartSerde(timestamps, IndexIO.BYTE_ORDER),
        timeBuilder,
        "__time"
    );

    /************* Write dimensions **************/
    dimIndex = 0;
    for (String dimension : mergedDimensions) {
      if (skippedDimensions.contains(dimension)) {
        dimIndex++;
        continue;
      }

      final ColumnDescriptor.Builder dimBuilder = ColumnDescriptor.builder();
      dimBuilder.setValueType(ValueType.STRING);

      final List<ByteBuffer> outParts = Lists.newArrayList();

      ByteArrayOutputStream nameBAOS = new ByteArrayOutputStream();
      serializerUtils.writeString(nameBAOS, dimension);
      outParts.add(ByteBuffer.wrap(nameBAOS.toByteArray()));

      boolean hasMultipleValues = columnCapabilities.get(dimension).hasMultipleValues();
      dimBuilder.setHasMultipleValues(hasMultipleValues);

      // make dimension columns
      VSizeIndexedInts singleValCol = null;
      VSizeIndexed multiValCol = null;

      ColumnAdder adder = hasMultipleValues ? new MultiValColumnAdder() : new SingleValColumnAdder();

      ConciseSet nullSet = null;
      rowCount = 0;
      for (Rowboat theRow : theRows) {
        if (dimIndex > theRow.getDims().length) {
          if (nullSet == null) {
            nullSet = new ConciseSet();
          }
          nullSet.add(rowCount);
          adder.add(null);
        } else {
          int[] dimVals = theRow.getDims()[dimIndex];
          if (dimVals == null || dimVals.length == 0) {
            if (nullSet == null) {
              nullSet = new ConciseSet();
            }
            nullSet.add(rowCount);
          }
          adder.add(dimVals);
        }
        rowCount++;
      }

      GenericIndexed<String> dictionary = null;
      final Iterable<String> dimensionValues = dimensionValuesLookup.get(dimension);
      boolean bumpDictionary = false;

      if (hasMultipleValues) {
        List<List<Integer>> vals = ((MultiValColumnAdder) adder).get();
        multiValCol = VSizeIndexed.fromIterable(
            Iterables.transform(
                vals,
                new Function<List<Integer>, VSizeIndexedInts>()
                {
                  @Override
                  public VSizeIndexedInts apply(List<Integer> input)
                  {
                    return VSizeIndexedInts.fromList(
                        input,
                        Collections.max(input)
                    );
                  }
                }
            )
        );
        dictionary = GenericIndexed.fromIterable(
            dimensionValues,
            GenericIndexed.stringStrategy
        );
      } else {
        final List<Integer> vals = ((SingleValColumnAdder) adder).get();

        if (nullSet != null) {
          log.info("Dimension[%s] has null rows.", dimension);

          if (Iterables.getFirst(dimensionValues, "") != null) {
            bumpDictionary = true;
            log.info("Dimension[%s] has no null value in the dictionary, expanding...", dimension);

            final List<String> nullList = Lists.newArrayList();
            nullList.add(null);

            dictionary = GenericIndexed.fromIterable(
                Iterables.concat(nullList, dimensionValues),
                GenericIndexed.stringStrategy
            );
            singleValCol = VSizeIndexedInts.fromList(
                new AbstractList<Integer>()
                {
                  @Override
                  public Integer get(int index)
                  {
                    Integer val = vals.get(index);
                    if (val == null) {
                      return 0;
                    }
                    return val + 1;
                  }

                  @Override
                  public int size()
                  {
                    return vals.size();
                  }
                }, dictionary.size()
            );
          }
        } else {
          dictionary = GenericIndexed.fromIterable(
              dimensionValues,
              GenericIndexed.stringStrategy
          );
          singleValCol = VSizeIndexedInts.fromList(vals, dictionary.size());
        }
      }

      // Make bitmap indexes
      List<ConciseSet> conciseSets = Lists.newArrayList();
      for (String dimVal : dimensionValues) {
        List<Iterable<Integer>> convertedInverteds = Lists.newArrayListWithCapacity(adapters.size());
        for (int j = 0; j < adapters.size(); ++j) {
          convertedInverteds.add(
              new ConvertingIndexedInts(
                  adapters.get(j).getInverteds(dimension, dimVal), rowNumConversions.get(j)
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

        conciseSets.add(bitset);
      }

      GenericIndexed<ImmutableConciseSet> bitmaps;
      if (!hasMultipleValues) {
        if (nullSet != null) {
          final ImmutableConciseSet theNullSet = ImmutableConciseSet.newImmutableFromMutable(nullSet);
          if (bumpDictionary) {
            bitmaps = GenericIndexed.fromIterable(
                Iterables.concat(
                    Arrays.asList(theNullSet),
                    Iterables.transform(
                        conciseSets,
                        new Function<ConciseSet, ImmutableConciseSet>()
                        {
                          @Override
                          public ImmutableConciseSet apply(ConciseSet input)
                          {
                            return ImmutableConciseSet.newImmutableFromMutable(input);
                          }
                        }
                    )
                ),
                ConciseCompressedIndexedInts.objectStrategy
            );
          } else {
            Iterable<ImmutableConciseSet> immutableConciseSets = Iterables.transform(
                conciseSets,
                new Function<ConciseSet, ImmutableConciseSet>()
                {
                  @Override
                  public ImmutableConciseSet apply(ConciseSet input)
                  {
                    return ImmutableConciseSet.newImmutableFromMutable(input);
                  }
                }
            );

            bitmaps = GenericIndexed.fromIterable(
                Iterables.concat(
                    Arrays.asList(
                        ImmutableConciseSet.union(
                            theNullSet,
                            Iterables.getFirst(immutableConciseSets, null)
                        )
                    ),
                    Iterables.skip(immutableConciseSets, 1)
                ),
                ConciseCompressedIndexedInts.objectStrategy
            );
          }
        } else {
          bitmaps = GenericIndexed.fromIterable(
              Iterables.transform(
                  conciseSets,
                  new Function<ConciseSet, ImmutableConciseSet>()
                  {
                    @Override
                    public ImmutableConciseSet apply(ConciseSet input)
                    {
                      return ImmutableConciseSet.newImmutableFromMutable(input);
                    }
                  }
              ),
              ConciseCompressedIndexedInts.objectStrategy
          );
        }
      } else {
        bitmaps = GenericIndexed.fromIterable(
            Iterables.transform(
                conciseSets,
                new Function<ConciseSet, ImmutableConciseSet>()
                {
                  @Override
                  public ImmutableConciseSet apply(ConciseSet input)
                  {
                    return ImmutableConciseSet.newImmutableFromMutable(input);
                  }
                }
            ),
            ConciseCompressedIndexedInts.objectStrategy
        );
      }

      // Make spatial indexes
      ImmutableRTree spatialIndex = null;
      boolean hasSpatialIndexes = columnCapabilities.get(dimension).hasSpatialIndexes();
      RTree tree = null;
      if (hasSpatialIndexes) {
        tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50));
      }

      int dimValIndex = 0;
      for (String dimVal : dimensionValuesLookup.get(dimension)) {
        if (hasSpatialIndexes) {
          List<String> stringCoords = Lists.newArrayList(SPLITTER.split(dimVal));
          float[] coords = new float[stringCoords.size()];
          for (int j = 0; j < coords.length; j++) {
            coords[j] = Float.valueOf(stringCoords.get(j));
          }
          tree.insert(coords, conciseSets.get(dimValIndex++));
        }
      }
      if (hasSpatialIndexes) {
        spatialIndex = ImmutableRTree.newImmutableFromMutable(tree);
      }

      writeColumn(
          mapper,
          v9Smoosher,
          new DictionaryEncodedColumnPartSerde(
              dictionary,
              singleValCol,
              multiValCol,
              bitmaps,
              spatialIndex
          ),
          dimBuilder,
          dimension
      );

      dimIndex++;
    }

    int metIndex = 0;
    for (String metric : mergedMetrics) {
      final ColumnDescriptor.Builder metBuilder = ColumnDescriptor.builder();

      ValueType type = valueTypes.get(metric);

      final int metricIndex = metIndex;
      switch (type) {
        case FLOAT:
          metBuilder.setValueType(ValueType.FLOAT);

          float[] arr = new float[rowCount];
          rowNum = 0;
          for (Rowboat theRow : theRows) {
            Object obj = theRow.getMetrics()[metricIndex]; // TODO
            arr[rowNum++] = (obj == null) ? 0 : ((Number) obj).floatValue();
          }

          CompressedFloatsIndexedSupplier compressedFloats = CompressedFloatsIndexedSupplier.fromFloatBuffer(
              FloatBuffer.wrap(arr),
              IndexIO.BYTE_ORDER
          );

          writeColumn(
              mapper,
              v9Smoosher,
              new FloatGenericColumnPartSerde(compressedFloats, IndexIO.BYTE_ORDER),
              metBuilder,
              metric
          );
          break;
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
                      return input.getMetrics()[metricIndex]; // TODO
                    }
                  }
              ),
              serde.getObjectStrategy()
          );

          metBuilder.setValueType(ValueType.COMPLEX);
          writeColumn(
              mapper,
              v9Smoosher,
              new ComplexColumnPartSerde(metricColumn, complexType),
              metBuilder,
              metric
          );
          break;
        default:
          throw new ISE("Unknown type[%s]", type);
      }
      metIndex++;
    }

    /*************  Main index.drd file **************/
    progress.progress();
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

    GenericIndexed<String> cols = GenericIndexed.fromIterable(finalColumns, GenericIndexed.stringStrategy);
    GenericIndexed<String> dims = GenericIndexed.fromIterable(finalDimensions, GenericIndexed.stringStrategy);

    final long numBytes = cols.getSerializedSize() + dims.getSerializedSize() + 16;
    final SmooshedWriter writer = v9Smoosher.addWithSmooshedWriter("index.drd", numBytes);

    cols.writeToChannel(writer);
    dims.writeToChannel(writer);

    DateTime minTime = new DateTime(Long.MAX_VALUE);
    DateTime maxTime = new DateTime(0l);

    for (IndexableAdapter index : adapters) {
      minTime = JodaUtils.minDateTime(minTime, index.getDataInterval().getStart());
      maxTime = JodaUtils.maxDateTime(maxTime, index.getDataInterval().getEnd());
    }
    final Interval dataInterval = new Interval(minTime, maxTime);

    serializerUtils.writeLong(writer, dataInterval.getStartMillis());
    serializerUtils.writeLong(writer, dataInterval.getEndMillis());
    writer.close();

    IndexIO.checkFileSize(new File(outDir, "index.drd"));
    log.info("outDir[%s] completed index.drd in %,d millis.", outDir, System.currentTimeMillis() - startTime);

    v9Smoosher.close();

    return outDir;
  }

  private static void writeColumn(
      ObjectMapper mapper,
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

  private static Iterable<Rowboat> makeRowIterable(
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
        dimLookup[count] = mergedDimensions.indexOf(dim.toLowerCase());
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
      return Iterators.transform(
          index.iterator(),
          new Function<Rowboat, Rowboat>()
          {
            @Override
            public Rowboat apply(Rowboat input)
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
                  if (converter.hasRemaining()) {
                    newDims[i][j] = converter.get(dims[i][j]);
                  }
                  if (!converter.hasRemaining()) {
                    log.error("Converter mismatch! wtfbbq!");
                  }
                  //newDims[i][j] = converter.get(dims[i][j]);
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
          lhs.getMetrics(),
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

  public static interface ProgressIndicator
  {
    public void progress();
  }

  private static class NoopProgressIndicator implements ProgressIndicator
  {
    @Override
    public void progress() {}
  }

  private static interface ColumnAdder
  {
    public void add(int[] vals);
  }

  private static class SingleValColumnAdder implements ColumnAdder
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

  private static class MultiValColumnAdder implements ColumnAdder
  {
    private final List<List<Integer>> data = Lists.newArrayList();

    public void add(int[] vals)
    {
      data.add(Ints.asList(vals));
    }

    public List<List<Integer>> get()
    {
      return data;
    }
  }
}
