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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.io.smoosh.FileSmoosher;
import com.metamx.common.io.smoosh.Smoosh;
import com.metamx.common.io.smoosh.SmooshedFileMapper;
import com.metamx.common.io.smoosh.SmooshedWriter;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.EmittingLogger;
import io.druid.common.utils.SerializerUtils;
import io.druid.guice.ConfigProvider;
import io.druid.guice.GuiceInjectors;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.DruidProcessingConfig;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.ArrayIndexed;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.ConciseCompressedIndexedInts;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.VSizeIndexed;
import io.druid.segment.data.VSizeIndexedInts;
import io.druid.segment.serde.BitmapIndexColumnPartSupplier;
import io.druid.segment.serde.ComplexColumnPartSerde;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import io.druid.segment.serde.DictionaryEncodedColumnSupplier;
import io.druid.segment.serde.FloatGenericColumnPartSerde;
import io.druid.segment.serde.FloatGenericColumnSupplier;
import io.druid.segment.serde.LongGenericColumnPartSerde;
import io.druid.segment.serde.LongGenericColumnSupplier;
import io.druid.segment.serde.SpatialIndexColumnPartSupplier;
import it.uniroma3.mat.extendedset.intset.ConciseSet;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import org.joda.time.Interval;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This should be changed to use DI instead of a static reference...
 */
public class IndexIO
{
  public static final byte V8_VERSION = 0x8;
  public static final byte V9_VERSION = 0x9;
  public static final int CURRENT_VERSION_ID = V9_VERSION;

  public static final ByteOrder BYTE_ORDER = ByteOrder.nativeOrder();

  private static final Map<Integer, IndexLoader> indexLoaders =
      ImmutableMap.<Integer, IndexLoader>builder()
                  .put(0, new LegacyIndexLoader())
                  .put(1, new LegacyIndexLoader())
                  .put(2, new LegacyIndexLoader())
                  .put(3, new LegacyIndexLoader())
                  .put(4, new LegacyIndexLoader())
                  .put(5, new LegacyIndexLoader())
                  .put(6, new LegacyIndexLoader())
                  .put(7, new LegacyIndexLoader())
                  .put(8, new LegacyIndexLoader())
                  .put(9, new V9IndexLoader())
                  .build();

  private static final EmittingLogger log = new EmittingLogger(IndexIO.class);
  private static final SerializerUtils serializerUtils = new SerializerUtils();

  private static final ObjectMapper mapper;
  protected static final ColumnConfig columnConfig;

  static {
    final Injector injector = GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                ConfigProvider.bind(
                    binder,
                    DruidProcessingConfig.class,
                    ImmutableMap.of("base_path", "druid.processing")
                );
                binder.bind(ColumnConfig.class).to(DruidProcessingConfig.class);
              }
            }
        )
    );
    mapper = injector.getInstance(ObjectMapper.class);
    columnConfig = injector.getInstance(ColumnConfig.class);
  }

  private static volatile IndexIOHandler handler = null;

  @Deprecated
  public static MMappedIndex mapDir(final File inDir) throws IOException
  {
    init();
    return handler.mapDir(inDir);
  }

  public static QueryableIndex loadIndex(File inDir) throws IOException
  {
    init();
    final int version = SegmentUtils.getVersionFromDir(inDir);

    final IndexLoader loader = indexLoaders.get(version);

    if (loader != null) {
      return loader.load(inDir);
    } else {
      throw new ISE("Unknown index version[%s]", version);
    }
  }

  public static boolean hasHandler()
  {
    return (IndexIO.handler != null);
  }

  public static void registerHandler(IndexIOHandler handler)
  {
    if (IndexIO.handler == null) {
      IndexIO.handler = handler;
    } else {
      throw new ISE("Already have a handler[%s], cannot register another[%s]", IndexIO.handler, handler);
    }
  }

  private static void init()
  {
    if (handler == null) {
      handler = new DefaultIndexIOHandler();
    }
  }

  public static int getVersionFromDir(File inDir) throws IOException
  {
    File versionFile = new File(inDir, "version.bin");
    if (versionFile.exists()) {
      return Ints.fromByteArray(Files.toByteArray(versionFile));
    }

    final File indexFile = new File(inDir, "index.drd");
    int version;
    try (InputStream in = new FileInputStream(indexFile)) {
      version = in.read();
    }
    return version;
  }

  public static void checkFileSize(File indexFile) throws IOException
  {
    final long fileSize = indexFile.length();
    if (fileSize > Integer.MAX_VALUE) {
      throw new IOException(String.format("File[%s] too large[%s]", indexFile, fileSize));
    }
  }

  public static boolean convertSegment(File toConvert, File converted) throws IOException
  {
    final int version = SegmentUtils.getVersionFromDir(toConvert);

    switch (version) {
      case 1:
      case 2:
      case 3:
        log.makeAlert("Attempt to load segment of version <= 3.")
           .addData("version", version)
           .emit();
        return false;
      case 4:
      case 5:
      case 6:
      case 7:
        log.info("Old version, re-persisting.");
        IndexMerger.append(
            Arrays.<IndexableAdapter>asList(new QueryableIndexIndexableAdapter(loadIndex(toConvert))),
            converted
        );
        return true;
      case 8:
        DefaultIndexIOHandler.convertV8toV9(toConvert, converted);
        return true;
      default:
        log.info("Version[%s], skipping.", version);
        return false;
    }
  }

  public static interface IndexIOHandler
  {
    public MMappedIndex mapDir(File inDir) throws IOException;
  }

  public static class DefaultIndexIOHandler implements IndexIOHandler
  {
    private static final Logger log = new Logger(DefaultIndexIOHandler.class);

    @Override
    public MMappedIndex mapDir(File inDir) throws IOException
    {
      log.debug("Mapping v8 index[%s]", inDir);
      long startTime = System.currentTimeMillis();

      InputStream indexIn = null;
      try {
        indexIn = new FileInputStream(new File(inDir, "index.drd"));
        byte theVersion = (byte) indexIn.read();
        if (theVersion != V8_VERSION) {
          throw new IllegalArgumentException(String.format("Unknown version[%s]", theVersion));
        }
      }
      finally {
        Closeables.close(indexIn, false);
      }

      SmooshedFileMapper smooshedFiles = Smoosh.map(inDir);
      ByteBuffer indexBuffer = smooshedFiles.mapFile("index.drd");

      indexBuffer.get(); // Skip the version byte
      final GenericIndexed<String> availableDimensions = GenericIndexed.read(
          indexBuffer, GenericIndexed.stringStrategy
      );
      final GenericIndexed<String> availableMetrics = GenericIndexed.read(
          indexBuffer, GenericIndexed.stringStrategy
      );
      final Interval dataInterval = new Interval(serializerUtils.readString(indexBuffer));

      CompressedLongsIndexedSupplier timestamps = CompressedLongsIndexedSupplier.fromByteBuffer(
          smooshedFiles.mapFile(makeTimeFile(inDir, BYTE_ORDER).getName()), BYTE_ORDER
      );

      Map<String, MetricHolder> metrics = Maps.newLinkedHashMap();
      for (String metric : availableMetrics) {
        final String metricFilename = makeMetricFile(inDir, metric, BYTE_ORDER).getName();
        final MetricHolder holder = MetricHolder.fromByteBuffer(smooshedFiles.mapFile(metricFilename));

        if (!metric.equals(holder.getName())) {
          throw new ISE("Metric[%s] loaded up metric[%s] from disk.  File names do matter.", metric, holder.getName());
        }
        metrics.put(metric, holder);
      }

      Map<String, GenericIndexed<String>> dimValueLookups = Maps.newHashMap();
      Map<String, VSizeIndexed> dimColumns = Maps.newHashMap();
      Map<String, GenericIndexed<ImmutableConciseSet>> invertedIndexed = Maps.newHashMap();

      for (String dimension : IndexedIterable.create(availableDimensions)) {
        ByteBuffer dimBuffer = smooshedFiles.mapFile(makeDimFile(inDir, dimension).getName());
        String fileDimensionName = serializerUtils.readString(dimBuffer);
        Preconditions.checkState(
            dimension.equals(fileDimensionName),
            "Dimension file[%s] has dimension[%s] in it!?",
            makeDimFile(inDir, dimension),
            fileDimensionName
        );

        dimValueLookups.put(dimension, GenericIndexed.read(dimBuffer, GenericIndexed.stringStrategy));
        dimColumns.put(dimension, VSizeIndexed.readFromByteBuffer(dimBuffer));
      }

      ByteBuffer invertedBuffer = smooshedFiles.mapFile("inverted.drd");
      for (int i = 0; i < availableDimensions.size(); ++i) {
        invertedIndexed.put(
            serializerUtils.readString(invertedBuffer),
            GenericIndexed.read(invertedBuffer, ConciseCompressedIndexedInts.objectStrategy)
        );
      }

      Map<String, ImmutableRTree> spatialIndexed = Maps.newHashMap();
      ByteBuffer spatialBuffer = smooshedFiles.mapFile("spatial.drd");
      while (spatialBuffer != null && spatialBuffer.hasRemaining()) {
        spatialIndexed.put(
            serializerUtils.readString(spatialBuffer),
            ByteBufferSerializer.read(spatialBuffer, IndexedRTree.objectStrategy)
        );
      }

      final MMappedIndex retVal = new MMappedIndex(
          availableDimensions,
          availableMetrics,
          dataInterval,
          timestamps,
          metrics,
          dimValueLookups,
          dimColumns,
          invertedIndexed,
          spatialIndexed,
          smooshedFiles
      );

      log.debug("Mapped v8 index[%s] in %,d millis", inDir, System.currentTimeMillis() - startTime);

      return retVal;
    }

    public static void convertV8toV9(File v8Dir, File v9Dir) throws IOException
    {
      log.info("Converting v8[%s] to v9[%s]", v8Dir, v9Dir);

      InputStream indexIn = null;
      try {
        indexIn = new FileInputStream(new File(v8Dir, "index.drd"));
        byte theVersion = (byte) indexIn.read();
        if (theVersion != V8_VERSION) {
          throw new IAE("Unknown version[%s]", theVersion);
        }
      }
      finally {
        Closeables.close(indexIn, false);
      }

      SmooshedFileMapper v8SmooshedFiles = Smoosh.map(v8Dir);

      v9Dir.mkdirs();
      final FileSmoosher v9Smoosher = new FileSmoosher(v9Dir);

      ByteStreams.write(Ints.toByteArray(9), Files.newOutputStreamSupplier(new File(v9Dir, "version.bin")));
      Map<String, GenericIndexed<ImmutableConciseSet>> bitmapIndexes = Maps.newHashMap();

      final ByteBuffer invertedBuffer = v8SmooshedFiles.mapFile("inverted.drd");
      while (invertedBuffer.hasRemaining()) {
        bitmapIndexes.put(
            serializerUtils.readString(invertedBuffer),
            GenericIndexed.read(invertedBuffer, ConciseCompressedIndexedInts.objectStrategy)
        );
      }

      Map<String, ImmutableRTree> spatialIndexes = Maps.newHashMap();
      final ByteBuffer spatialBuffer = v8SmooshedFiles.mapFile("spatial.drd");
      while (spatialBuffer != null && spatialBuffer.hasRemaining()) {
        spatialIndexes.put(
            serializerUtils.readString(spatialBuffer),
            ByteBufferSerializer.read(spatialBuffer, IndexedRTree.objectStrategy)
        );
      }

      final LinkedHashSet<String> skippedFiles = Sets.newLinkedHashSet();
      final Set<String> skippedDimensions = Sets.newLinkedHashSet();
      for (String filename : v8SmooshedFiles.getInternalFilenames()) {
        log.info("Processing file[%s]", filename);
        if (filename.startsWith("dim_")) {
          final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
          builder.setValueType(ValueType.STRING);

          final List<ByteBuffer> outParts = Lists.newArrayList();

          ByteBuffer dimBuffer = v8SmooshedFiles.mapFile(filename);
          String dimension = serializerUtils.readString(dimBuffer);
          if (!filename.equals(String.format("dim_%s.drd", dimension))) {
            throw new ISE("loaded dimension[%s] from file[%s]", dimension, filename);
          }

          ByteArrayOutputStream nameBAOS = new ByteArrayOutputStream();
          serializerUtils.writeString(nameBAOS, dimension);
          outParts.add(ByteBuffer.wrap(nameBAOS.toByteArray()));

          GenericIndexed<String> dictionary = GenericIndexed.read(
              dimBuffer, GenericIndexed.stringStrategy
          );

          if (dictionary.size() == 0) {
            log.info("Dimension[%s] had cardinality 0, equivalent to no column, so skipping.", dimension);
            skippedDimensions.add(dimension);
            continue;
          }

          VSizeIndexedInts singleValCol = null;
          VSizeIndexed multiValCol = VSizeIndexed.readFromByteBuffer(dimBuffer.asReadOnlyBuffer());
          GenericIndexed<ImmutableConciseSet> bitmaps = bitmapIndexes.get(dimension);
          ImmutableRTree spatialIndex = spatialIndexes.get(dimension);

          boolean onlyOneValue = true;
          ConciseSet nullsSet = null;
          for (int i = 0; i < multiValCol.size(); ++i) {
            VSizeIndexedInts rowValue = multiValCol.get(i);
            if (!onlyOneValue) {
              break;
            }
            if (rowValue.size() > 1) {
              onlyOneValue = false;
            }
            if (rowValue.size() == 0) {
              if (nullsSet == null) {
                nullsSet = new ConciseSet();
              }
              nullsSet.add(i);
            }
          }

          if (onlyOneValue) {
            log.info("Dimension[%s] is single value, converting...", dimension);
            final boolean bumpedDictionary;
            if (nullsSet != null) {
              log.info("Dimension[%s] has null rows.", dimension);
              final ImmutableConciseSet theNullSet = ImmutableConciseSet.newImmutableFromMutable(nullsSet);

              if (dictionary.get(0) != null) {
                log.info("Dimension[%s] has no null value in the dictionary, expanding...", dimension);
                bumpedDictionary = true;
                final List<String> nullList = Lists.newArrayList();
                nullList.add(null);

                dictionary = GenericIndexed.fromIterable(
                    Iterables.concat(nullList, dictionary),
                    GenericIndexed.stringStrategy
                );

                bitmaps = GenericIndexed.fromIterable(
                    Iterables.concat(Arrays.asList(theNullSet), bitmaps),
                    ConciseCompressedIndexedInts.objectStrategy
                );
              } else {
                bumpedDictionary = false;
                bitmaps = GenericIndexed.fromIterable(
                    Iterables.concat(
                        Arrays.asList(ImmutableConciseSet.union(theNullSet, bitmaps.get(0))),
                        Iterables.skip(bitmaps, 1)
                    ),
                    ConciseCompressedIndexedInts.objectStrategy
                );
              }
            } else {
              bumpedDictionary = false;
            }

            final VSizeIndexed finalMultiValCol = multiValCol;
            singleValCol = VSizeIndexedInts.fromList(
                new AbstractList<Integer>()
                {
                  @Override
                  public Integer get(int index)
                  {
                    final VSizeIndexedInts ints = finalMultiValCol.get(index);
                    return ints.size() == 0 ? 0 : ints.get(0) + (bumpedDictionary ? 1 : 0);
                  }

                  @Override
                  public int size()
                  {
                    return finalMultiValCol.size();
                  }
                },
                dictionary.size()
            );
            multiValCol = null;
          } else {
            builder.setHasMultipleValues(true);
          }

          builder.addSerde(
              new DictionaryEncodedColumnPartSerde(
                  dictionary,
                  singleValCol,
                  multiValCol,
                  bitmaps,
                  spatialIndex
              )
          );

          final ColumnDescriptor serdeficator = builder.build();

          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          serializerUtils.writeString(baos, mapper.writeValueAsString(serdeficator));
          byte[] specBytes = baos.toByteArray();

          final SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter(
              dimension, serdeficator.numBytes() + specBytes.length
          );
          channel.write(ByteBuffer.wrap(specBytes));
          serdeficator.write(channel);
          channel.close();
        } else if (filename.startsWith("met_")) {
          if (!filename.endsWith(String.format("%s.drd", BYTE_ORDER))) {
            skippedFiles.add(filename);
            continue;
          }

          MetricHolder holder = MetricHolder.fromByteBuffer(v8SmooshedFiles.mapFile(filename));
          final String metric = holder.getName();

          final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();

          switch (holder.getType()) {
            case FLOAT:
              builder.setValueType(ValueType.FLOAT);
              builder.addSerde(new FloatGenericColumnPartSerde(holder.floatType, BYTE_ORDER));
              break;
            case COMPLEX:
              if (!(holder.complexType instanceof GenericIndexed)) {
                throw new ISE("Serialized complex types must be GenericIndexed objects.");
              }
              final GenericIndexed column = (GenericIndexed) holder.complexType;
              final String complexType = holder.getTypeName();

              builder.setValueType(ValueType.COMPLEX);
              builder.addSerde(new ComplexColumnPartSerde(column, complexType));
              break;
            default:
              throw new ISE("Unknown type[%s]", holder.getType());
          }

          final ColumnDescriptor serdeficator = builder.build();

          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          serializerUtils.writeString(baos, mapper.writeValueAsString(serdeficator));
          byte[] specBytes = baos.toByteArray();

          final SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter(
              metric, serdeficator.numBytes() + specBytes.length
          );
          channel.write(ByteBuffer.wrap(specBytes));
          serdeficator.write(channel);
          channel.close();
        } else if (String.format("time_%s.drd", BYTE_ORDER).equals(filename)) {
          CompressedLongsIndexedSupplier timestamps = CompressedLongsIndexedSupplier.fromByteBuffer(
              v8SmooshedFiles.mapFile(filename), BYTE_ORDER
          );

          final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
          builder.setValueType(ValueType.LONG);
          builder.addSerde(new LongGenericColumnPartSerde(timestamps, BYTE_ORDER));

          final ColumnDescriptor serdeficator = builder.build();

          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          serializerUtils.writeString(baos, mapper.writeValueAsString(serdeficator));
          byte[] specBytes = baos.toByteArray();

          final SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter(
              "__time", serdeficator.numBytes() + specBytes.length
          );
          channel.write(ByteBuffer.wrap(specBytes));
          serdeficator.write(channel);
          channel.close();
        } else {
          skippedFiles.add(filename);
        }
      }

      final ByteBuffer indexBuffer = v8SmooshedFiles.mapFile("index.drd");

      indexBuffer.get(); // Skip the version byte
      final GenericIndexed<String> dims8 = GenericIndexed.read(
          indexBuffer, GenericIndexed.stringStrategy
      );
      final GenericIndexed<String> dims9 = GenericIndexed.fromIterable(
          Iterables.filter(
              dims8, new Predicate<String>()
          {
            @Override
            public boolean apply(String s)
            {
              return !skippedDimensions.contains(s);
            }
          }
          ),
          GenericIndexed.stringStrategy
      );
      final GenericIndexed<String> availableMetrics = GenericIndexed.read(
          indexBuffer, GenericIndexed.stringStrategy
      );
      final Interval dataInterval = new Interval(serializerUtils.readString(indexBuffer));

      Set<String> columns = Sets.newTreeSet();
      columns.addAll(Lists.newArrayList(dims9));
      columns.addAll(Lists.newArrayList(availableMetrics));

      GenericIndexed<String> cols = GenericIndexed.fromIterable(columns, GenericIndexed.stringStrategy);

      final long numBytes = cols.getSerializedSize() + dims9.getSerializedSize() + 16;
      final SmooshedWriter writer = v9Smoosher.addWithSmooshedWriter("index.drd", numBytes);
      cols.writeToChannel(writer);
      dims9.writeToChannel(writer);
      serializerUtils.writeLong(writer, dataInterval.getStartMillis());
      serializerUtils.writeLong(writer, dataInterval.getEndMillis());
      writer.close();

      log.info("Skipped files[%s]", skippedFiles);

      v9Smoosher.close();
    }
  }

  static interface IndexLoader
  {
    public QueryableIndex load(File inDir) throws IOException;
  }

  static class LegacyIndexLoader implements IndexLoader
  {
    @Override
    public QueryableIndex load(File inDir) throws IOException
    {
      MMappedIndex index = IndexIO.mapDir(inDir);

      Map<String, Column> columns = Maps.newHashMap();

      for (String dimension : index.getAvailableDimensions()) {
        ColumnBuilder builder = new ColumnBuilder()
            .setType(ValueType.STRING)
            .setHasMultipleValues(true)
            .setDictionaryEncodedColumn(
                new DictionaryEncodedColumnSupplier(
                    index.getDimValueLookup(dimension),
                    null,
                    index.getDimColumn(dimension),
                    columnConfig.columnCacheSizeBytes()
                )
            )
            .setBitmapIndex(
                new BitmapIndexColumnPartSupplier(
                    index.getInvertedIndexes().get(dimension), index.getDimValueLookup(dimension)
                )
            );
        if (index.getSpatialIndexes().get(dimension) != null) {
          builder.setSpatialIndex(
              new SpatialIndexColumnPartSupplier(
                  index.getSpatialIndexes().get(dimension)
              )
          );
        }
        columns.put(
            dimension.toLowerCase(),
            builder.build()
        );
      }

      for (String metric : index.getAvailableMetrics()) {
        final MetricHolder metricHolder = index.getMetricHolder(metric);
        if (metricHolder.getType() == MetricHolder.MetricType.FLOAT) {
          columns.put(
              metric.toLowerCase(),
              new ColumnBuilder()
                  .setType(ValueType.FLOAT)
                  .setGenericColumn(new FloatGenericColumnSupplier(metricHolder.floatType, BYTE_ORDER))
                  .build()
          );
        } else if (metricHolder.getType() == MetricHolder.MetricType.COMPLEX) {
          columns.put(
              metric.toLowerCase(),
              new ColumnBuilder()
                  .setType(ValueType.COMPLEX)
                  .setComplexColumn(
                      new ComplexColumnPartSupplier(
                          metricHolder.getTypeName(), (GenericIndexed) metricHolder.complexType
                      )
                  )
                  .build()
          );
        }
      }

      Set<String> colSet = Sets.newTreeSet();
      for (String dimension : index.getAvailableDimensions()) {
        colSet.add(dimension.toLowerCase());
      }
      for (String metric : index.getAvailableMetrics()) {
        colSet.add(metric.toLowerCase());
      }

      String[] cols = colSet.toArray(new String[colSet.size()]);

      return new SimpleQueryableIndex(
          index.getDataInterval(),
          new ArrayIndexed<String>(cols, String.class),
          index.getAvailableDimensions(),
          new ColumnBuilder()
              .setType(ValueType.LONG)
              .setGenericColumn(new LongGenericColumnSupplier(index.timestamps))
              .build(),
          columns,
          index.getFileMapper()
      );
    }
  }

  static class V9IndexLoader implements IndexLoader
  {
    @Override
    public QueryableIndex load(File inDir) throws IOException
    {
      log.debug("Mapping v9 index[%s]", inDir);
      long startTime = System.currentTimeMillis();

      final int theVersion = Ints.fromByteArray(Files.toByteArray(new File(inDir, "version.bin")));
      if (theVersion != V9_VERSION) {
        throw new IllegalArgumentException(String.format("Expected version[9], got[%s]", theVersion));
      }

      SmooshedFileMapper smooshedFiles = Smoosh.map(inDir);

      ByteBuffer indexBuffer = smooshedFiles.mapFile("index.drd");
      final GenericIndexed<String> cols = GenericIndexed.read(indexBuffer, GenericIndexed.stringStrategy);
      final GenericIndexed<String> dims = GenericIndexed.read(indexBuffer, GenericIndexed.stringStrategy);
      final Interval dataInterval = new Interval(indexBuffer.getLong(), indexBuffer.getLong());

      Map<String, Column> columns = Maps.newHashMap();

      ObjectMapper mapper = new DefaultObjectMapper();

      for (String columnName : cols) {
        columns.put(columnName, deserializeColumn(mapper, smooshedFiles.mapFile(columnName)));
      }

      final QueryableIndex index = new SimpleQueryableIndex(
          dataInterval, cols, dims, deserializeColumn(mapper, smooshedFiles.mapFile("__time")), columns, smooshedFiles
      );

      log.debug("Mapped v9 index[%s] in %,d millis", inDir, System.currentTimeMillis() - startTime);

      return index;
    }

    private Column deserializeColumn(ObjectMapper mapper, ByteBuffer byteBuffer) throws IOException
    {
      ColumnDescriptor serde = mapper.readValue(
          serializerUtils.readString(byteBuffer), ColumnDescriptor.class
      );
      return serde.read(byteBuffer, columnConfig);
    }
  }

  public static File makeDimFile(File dir, String dimension)
  {
    return new File(dir, String.format("dim_%s.drd", dimension));
  }

  public static File makeTimeFile(File dir, ByteOrder order)
  {
    return new File(dir, String.format("time_%s.drd", order));
  }

  public static File makeMetricFile(File dir, String metricName, ByteOrder order)
  {
    return new File(dir, String.format("met_%s_%s.drd", metricName, order));
  }
}
