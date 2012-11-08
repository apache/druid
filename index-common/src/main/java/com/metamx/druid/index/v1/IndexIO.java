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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.metamx.common.ISE;
import com.metamx.common.io.smoosh.Smoosh;
import com.metamx.common.io.smoosh.SmooshedFileMapper;
import com.metamx.common.logger.Logger;
import com.metamx.druid.kv.ConciseCompressedIndexedInts;
import com.metamx.druid.kv.GenericIndexed;
import com.metamx.druid.kv.IndexedIterable;
import com.metamx.druid.kv.VSizeIndexed;
import com.metamx.druid.utils.SerializerUtils;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import org.joda.time.Interval;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

/**
 * This should be changed to use DI instead of a static reference...
 */
public class IndexIO
{
  private static volatile IndexIOHandler handler = null;
  public static final byte CURRENT_VERSION_ID = 0x8;

  public static Index readIndex(File inDir) throws IOException
  {
    init();
    return handler.readIndex(inDir);
  }

  public static boolean canBeMapped(final File inDir) throws IOException
  {
    init();
    return handler.canBeMapped(inDir);
  }

  public static MMappedIndex mapDir(final File inDir) throws IOException
  {
    init();
    return handler.mapDir(inDir);
  }

  public static void storeLatest(Index index, File file) throws IOException
  {
    handler.storeLatest(index, file);
  }

  public static boolean hasHandler()
  {
    return (IndexIO.handler != null);
  }

  public static void registerHandler(IndexIOHandler handler)
  {
    if (IndexIO.handler == null) {
      IndexIO.handler = handler;
    }
    else {
      throw new ISE("Already have a handler[%s], cannot register another[%s]", IndexIO.handler, handler);
    }
  }

  private static void init()
  {
    if (handler == null) {
      handler = new DefaultIndexIOHandler();
    }
  }

  public static void checkFileSize(File indexFile) throws IOException
  {
    final long fileSize = indexFile.length();
    if (fileSize > Integer.MAX_VALUE) {
      throw new IOException(String.format("File[%s] too large[%s]", indexFile, fileSize));
    }
  }

  public static interface IndexIOHandler
  {
    /**
     * This only exists for some legacy compatibility reasons, Metamarkets is working on getting rid of it in
     * future versions
     *
     * @param inDir
     *
     * @return
     */
    public Index readIndex(File inDir) throws IOException;

    /**
     * This should really always return true, but it exists for legacy compatibility reasons, Metamarkets
     * is working on getting rid of it in future versions
     *
     * @return
     */
    public boolean canBeMapped(File inDir) throws IOException;
    public MMappedIndex mapDir(File inDir) throws IOException;

    /**
     * This only exists for some legacy compatibility reasons, Metamarkets is working on getting rid of it in
     * future versions.  Normal persisting of indexes is done via IndexMerger.
     *
     *
     * @param file
     */
    public void storeLatest(Index index, File file) throws IOException;
  }

  static class DefaultIndexIOHandler implements IndexIOHandler
  {
    private static final Logger log = new Logger(DefaultIndexIOHandler.class);
    private static final SerializerUtils serializerUtils = new SerializerUtils();
    private static final ByteOrder BYTE_ORDER = ByteOrder.nativeOrder();

    @Override
    public Index readIndex(File inDir)
    {
      throw new UnsupportedOperationException("Shouldn't ever happen in a cluster that is not owned by MMX.");
    }

    @Override
    public boolean canBeMapped(File inDir)
    {
      return true;
    }

    public static final byte VERSION_ID = 0x8;

    @Override
    public MMappedIndex mapDir(File inDir) throws IOException
    {
      log.debug("Mapping v8 index[%s]", inDir);
      long startTime = System.currentTimeMillis();

      InputStream indexIn = null;
      try {
        indexIn = new FileInputStream(new File(inDir, "index.drd"));
        byte theVersion = (byte) indexIn.read();
        if (theVersion != VERSION_ID) {
          throw new IllegalArgumentException(String.format("Unknown version[%s]", theVersion));
        }
      }
      finally {
        Closeables.close(indexIn, false);
      }

      SmooshedFileMapper smooshedFiles = Smoosh.map(inDir);
      ByteBuffer indexBuffer = smooshedFiles.mapFile("index.drd");

      indexBuffer.get(); // Skip the version byte
      final GenericIndexed<String> availableDimensions = GenericIndexed.readFromByteBuffer(
          indexBuffer, GenericIndexed.stringStrategy
      );
      final GenericIndexed<String> availableMetrics = GenericIndexed.readFromByteBuffer(
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

        dimValueLookups.put(dimension, GenericIndexed.readFromByteBuffer(dimBuffer, GenericIndexed.stringStrategy));
        dimColumns.put(dimension, VSizeIndexed.readFromByteBuffer(dimBuffer));
      }

      ByteBuffer invertedBuffer = smooshedFiles.mapFile("inverted.drd");
      for (int i = 0; i < availableDimensions.size(); ++i) {
        invertedIndexed.put(
            serializerUtils.readString(invertedBuffer),
            GenericIndexed.readFromByteBuffer(invertedBuffer, ConciseCompressedIndexedInts.objectStrategy)
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
          invertedIndexed
      );

      log.debug("Mapped v8 index[%s] in %,d millis", inDir, System.currentTimeMillis() - startTime);

      return retVal;
    }

    @Override
    public void storeLatest(Index index, File file)
    {
      throw new UnsupportedOperationException("Shouldn't ever happen in a cluster that is not owned by MMX.");
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
