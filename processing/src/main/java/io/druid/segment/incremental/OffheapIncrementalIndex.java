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

package io.druid.segment.incremental;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.InputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import org.apache.commons.io.FileUtils;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.MappedFileVolWrapper;
import org.mapdb.Serializer;
import org.mapdb.StoreDirect;
import org.mapdb.Volume;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class OffheapIncrementalIndex extends IncrementalIndex<BufferAggregator>
{
  private static final Logger log = new Logger(OffheapIncrementalIndex.class);

  private final DiskBBFactory aggBBFactory;

  private final List<ByteBuffer> aggBuffers = new ArrayList<>();
  private final List<int[]> indexAndOffsets = new ArrayList<>();

  private final DB factsDb;
  private final ConcurrentMap<TimeAndDims, Integer> facts;


  private final AtomicInteger indexIncrement = new AtomicInteger(0);

  private volatile Map<String, ColumnSelectorFactory> selectors;

  //given a ByteBuffer and an offset where all aggregates for a row are stored
  //offset + aggOffsetInBuffer[i] would give position in ByteBuffer where ith aggregate
  //is stored
  private volatile int[] aggOffsetInBuffer;
  private volatile int aggsTotalSize;

  private String outOfRowsReason = null;

  private File tmpDirLocation;

  public OffheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      boolean sortFacts,
      long maxOffheapSize
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions, sortFacts);

    try {
      tmpDirLocation = Files.createTempDirectory("druid-offheap-index").toFile();

      File mapDbFile = File.createTempFile("mapdb", "", tmpDirLocation);
      File aggsBBFile = File.createTempFile("aggs", "", tmpDirLocation);

      AtomicLong availableTotalSpace = new AtomicLong(maxOffheapSize);

      final DBMaker dbMaker = new MyDBMaker(mapDbFile, 24, 27, availableTotalSpace)
          .deleteFilesAfterClose()
          .commitFileSyncDisable()
          .cacheSize(1024)
          .transactionDisable()
          .mmapFileEnable();

      this.factsDb = dbMaker.make();

      if (sortFacts) {
        TimeAndDimsBTreeKeySerializer timeAndDimsSerializer = new TimeAndDimsBTreeKeySerializer(this);
        this.facts = factsDb.createTreeMap("__facts" + UUID.randomUUID())
                            .keySerializer(timeAndDimsSerializer)
                            .comparator(timeAndDimsSerializer.getComparator())
                            .valueSerializer(Serializer.INTEGER)
                            .make();
      } else {
        this.facts = factsDb.createHashMap("__facts" + UUID.randomUUID())
                            .keySerializer(TimeAndDimsSerializer.INSTANCE)
                            .valueSerializer(Serializer.INTEGER)
                            .make();
      }

      this.aggBBFactory = new DiskBBFactory(aggsBBFile, 27, availableTotalSpace);

      //check that agg buffers can hold at least one row's aggregators
      ByteBuffer bb = this.aggBBFactory.allocateBB();
      if (bb.capacity() < aggsTotalSize) {
        throw new IAE("bufferPool buffers capacity must be >= [%s]", aggsTotalSize);
      }
      aggBuffers.add(bb);

    }
    catch (Exception ex) {
      FileUtils.deleteQuietly(tmpDirLocation);
      throw Throwables.propagate(ex);
    }
  }

  public OffheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      boolean sortFacts,
      long maxOffheapSize
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        deserializeComplexMetrics,
        reportParseExceptions,
        sortFacts,
        maxOffheapSize
    );
  }

  public OffheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      long maxOffheapSize
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        true,
        true,
        true,
        maxOffheapSize
    );
  }

  @Override
  public ConcurrentMap<TimeAndDims, Integer> getFacts()
  {
    return facts;
  }

  @Override
  protected DimDim makeDimDim(String dimension, Object lock)
  {
    return new OnheapIncrementalIndex.OnHeapDimDim(lock);
  }

  @Override
  protected BufferAggregator[] initAggs(
      AggregatorFactory[] metrics, Supplier<InputRow> rowSupplier, boolean deserializeComplexMetrics
  )
  {
    selectors = Maps.newHashMap();
    aggOffsetInBuffer = new int[metrics.length];

    for (int i = 0; i < metrics.length; i++) {
      AggregatorFactory agg = metrics[i];

      ColumnSelectorFactory columnSelectorFactory = makeColumnSelectorFactory(
          agg,
          rowSupplier,
          deserializeComplexMetrics
      );

      selectors.put(
          agg.getName(),
          new OnheapIncrementalIndex.ObjectCachingColumnSelectorFactory(columnSelectorFactory)
      );

      if (i == 0) {
        aggOffsetInBuffer[i] = 0;
      } else {
        aggOffsetInBuffer[i] = aggOffsetInBuffer[i-1] + metrics[i-1].getMaxIntermediateSize();
      }
    }

    aggsTotalSize = aggOffsetInBuffer[metrics.length - 1] + metrics[metrics.length - 1].getMaxIntermediateSize();

    return new BufferAggregator[metrics.length];
  }

  @Override
  protected Integer addToFacts(
      AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      InputRow row,
      AtomicInteger numEntries,
      TimeAndDims key,
      ThreadLocal<InputRow> rowContainer,
      Supplier<InputRow> rowSupplier
  ) throws IndexSizeExceededException
  {
    ByteBuffer aggBuffer;
    int bufferIndex;
    int bufferOffset;

    synchronized (this) {
      final Integer priorIndex = facts.get(key);
      if (null != priorIndex) {
        final int[] indexAndOffset = indexAndOffsets.get(priorIndex);
        bufferIndex = indexAndOffset[0];
        bufferOffset = indexAndOffset[1];
        aggBuffer = aggBuffers.get(bufferIndex);
      } else {
        if (metrics.length > 0 && getAggs()[0] == null) {
          // note: creation of Aggregators is done lazily when at least one row from input is available
          // so that FilteredAggregators could be initialized correctly.
          rowContainer.set(row);
          for (int i = 0; i < metrics.length; i++) {
            final AggregatorFactory agg = metrics[i];
            getAggs()[i] = agg.factorizeBuffered(
                makeColumnSelectorFactory(agg, rowSupplier, deserializeComplexMetrics)
            );
          }
          rowContainer.set(null);
        }

        bufferIndex = aggBuffers.size() - 1;
        ByteBuffer lastBuffer = aggBuffers.isEmpty() ? null : aggBuffers.get(aggBuffers.size() - 1);
        int[] lastAggregatorsIndexAndOffset = indexAndOffsets.isEmpty()
                                              ? null
                                              : indexAndOffsets.get(indexAndOffsets.size() - 1);

        if (lastAggregatorsIndexAndOffset != null && lastAggregatorsIndexAndOffset[0] != bufferIndex) {
          throw new ISE("last row's aggregate's buffer and last buffer index must be same");
        }

        bufferOffset = aggsTotalSize + (lastAggregatorsIndexAndOffset != null ? lastAggregatorsIndexAndOffset[1] : 0);
        if (lastBuffer != null &&
            lastBuffer.capacity() - bufferOffset >= aggsTotalSize) {
          aggBuffer = lastBuffer;
        } else {
          ByteBuffer bb;
          try {
            bb = aggBBFactory.allocateBB();
          } catch(Exception ex) {
            throw new IndexSizeExceededException(ex, "failed to allocated buffer for aggregation");
          }
          aggBuffers.add(bb);
          bufferIndex = aggBuffers.size() - 1;
          bufferOffset = 0;
          aggBuffer = bb;
        }

        for (int i = 0; i < metrics.length; i++) {
          getAggs()[i].init(aggBuffer, bufferOffset + aggOffsetInBuffer[i]);
        }

        final Integer rowIndex = indexIncrement.getAndIncrement();

        // note that indexAndOffsets must be updated before facts, because as soon as we update facts
        // concurrent readers get hold of it and might ask for newly added row
        indexAndOffsets.add(new int[]{bufferIndex, bufferOffset});

        Integer prev = null;
        try {
           prev = facts.putIfAbsent(key, rowIndex);
        } catch(Exception ex) {
          indexAndOffsets.remove(indexAndOffsets.size() - 1);
          throw new IndexSizeExceededException(ex, "failed to allocate buffer for storing facts");
        }

        if (null == prev) {
          numEntries.incrementAndGet();
        } else {
          throw new ISE("WTF! we are in sychronized block.");
        }
      }
    }

    rowContainer.set(row);

    for (int i = 0; i < metrics.length; i++) {
      final BufferAggregator agg = getAggs()[i];

      synchronized (agg) {
        try {
          agg.aggregate(aggBuffer, bufferOffset + aggOffsetInBuffer[i]);
        } catch (ParseException e) {
          // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
          if (reportParseExceptions) {
            throw new ParseException(e, "Encountered parse error for aggregator[%s]", getMetricAggs()[i].getName());
          } else {
            log.debug(e, "Encountered parse error, skipping aggregator[%s].", getMetricAggs()[i].getName());
          }
        }
      }
    }
    rowContainer.set(null);
    return numEntries.get();
  }

  @Override
  public boolean canAppendRow()
  {
    //For off-heap, its not possible to reliably implement this. Currently, Offheap is only used for groupBy merging
    //where this is not used.
    return true;
  }

  @Override
  public String getOutOfRowsReason()
  {
    return outOfRowsReason;
  }

  @Override
  protected BufferAggregator[] getAggsForRow(int rowOffset)
  {
    return getAggs();
  }

  @Override
  protected Object getAggVal(BufferAggregator agg, int rowOffset, int aggPosition)
  {
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]);
    return agg.get(bb, indexAndOffset[1] + aggOffsetInBuffer[aggPosition]);
  }

  @Override
  public float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]);
    return agg.getFloat(bb, indexAndOffset[1] + aggOffsetInBuffer[aggOffset]);
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]);
    return agg.getLong(bb, indexAndOffset[1] + aggOffsetInBuffer[aggOffset]);
  }

  @Override
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]);
    return agg.get(bb, indexAndOffset[1] + aggOffsetInBuffer[aggOffset]);
  }

  /**
   * NOTE: This is NOT thread-safe with add... so make sure all the adding is DONE before closing
   */
  @Override
  public void close()
  {
    //note that it is important to first clear mapDB stuff as it depends on dimLookups
    //in the comparator which should be available before the cleanup here.
    RuntimeException ex = null;
    try {
      factsDb.close();
    } catch (Exception e) {
      ex = Throwables.propagate(e);
    }

    indexAndOffsets.clear();
    aggBuffers.clear();

    try {
      FileUtils.deleteDirectory(tmpDirLocation);
    } catch(IOException ioe) {
      if (ex == null) {
        ex = Throwables.propagate(ioe);
      } else {
        ex.addSuppressed(ioe);
      }
    }

    if (selectors != null) {
      selectors.clear();
    }

    super.close();

    if (ex != null) {
      throw ex;
    }
  }

  private static class TimeAndDimsSerializer implements Serializer<TimeAndDims>, Serializable
  {
    private static final int[] EMPTY_INT_ARRAY = new int[0];

    public static final TimeAndDimsSerializer INSTANCE = new TimeAndDimsSerializer();

    private TimeAndDimsSerializer()
    {

    }

    @Override
    public void serialize(DataOutput out, TimeAndDims value) throws IOException
    {
      out.writeLong(value.getTimestamp());
      out.writeInt(value.getDims().length);
      for (int[] dims : value.getDims()) {
        if (dims == null) {
          writeArr(EMPTY_INT_ARRAY, out);
        } else {
          writeArr(dims, out);
        }
      }
    }

    @Override
    public TimeAndDims deserialize(DataInput in, int available) throws IOException
    {
      final long timeStamp = in.readLong();
      final int[][] dims = new int[in.readInt()][];

      for (int j = 0; j < dims.length; j++) {
        dims[j] = readArr(in);
      }

      return new TimeAndDims(timeStamp, dims);
    }

    @Override
    public int fixedSize()
    {
      return -1;
    }

    private void writeArr(int[] value, DataOutput out) throws IOException
    {
      out.writeInt(value.length);
      for (int v : value) {
        out.writeInt(v);
      }
    }

    private int[] readArr(DataInput in) throws IOException
    {
      int len = in.readInt();
      if (len == 0) {
        return EMPTY_INT_ARRAY;
      } else {
        int[] result = new int[len];
        for (int i = 0; i < len; i++) {
          result[i] = in.readInt();
        }
        return result;
      }
    }
  }

  private static class TimeAndDimsBTreeKeySerializer extends BTreeKeySerializer<TimeAndDims> implements Serializable
  {
    private final Comparator<TimeAndDims> comparator;

    TimeAndDimsBTreeKeySerializer(final OffheapIncrementalIndex offheapIncrementalIndex)
    {
      this.comparator = new OffHeapTimeAndDimsComparator(
          new Supplier<List<DimDim>>()
          {
            @Override
            public List<DimDim> get()
            {
              return offheapIncrementalIndex.dimValues;
            }
          }
      );
    }

    @Override
    public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException
    {
      for (int i = start; i < end; i++) {
        TimeAndDims timeAndDim = (TimeAndDims) keys[i];
        TimeAndDimsSerializer.INSTANCE.serialize(out, timeAndDim);
      }
    }

    @Override
    public Object[] deserialize(DataInput in, int start, int end, int size) throws IOException
    {
      Object[] ret = new Object[size];
      for (int i = start; i < end; i++) {
        ret[i] = TimeAndDimsSerializer.INSTANCE.deserialize(in, 0);
      }

      return ret;
    }

    @Override
    public Comparator<TimeAndDims> getComparator()
    {
      return comparator;
    }
  }

  private static class OffHeapTimeAndDimsComparator implements Comparator<TimeAndDims>, Serializable
  {
    private transient Supplier<List<DimDim>> dimDimsSupplier;

    OffHeapTimeAndDimsComparator(Supplier<List<DimDim>> dimDimsSupplier)
    {
      this.dimDimsSupplier = dimDimsSupplier;
    }

    @Override
    public int compare(TimeAndDims o1, TimeAndDims o2)
    {
      return new TimeAndDimsComp(dimDimsSupplier.get()).compare(o1, o2);
    }
  }

  private static class DiskBBFactory
  {
    private final long increments;
    private final MappedFileVolWrapper vol;

    private long offset = 0;

    public DiskBBFactory(File file, int chunkShift, AtomicLong availableSpace)
    {
      vol = new MappedFileVolWrapper(file, chunkShift, availableSpace);
      increments = 1L<<chunkShift;
    }

    public ByteBuffer allocateBB()
    {
      ByteBuffer value = vol.makeNewBuffer(offset);
      offset += increments;
      return value;
    }
  }

  private static class MyDBMaker extends DBMaker
  {
    private final AtomicLong availableSpace;
    private final int indexFileChunkShift;
    private final int dataFileChunkShift;

    public MyDBMaker(File file, int indexFileChunkShift, int dataFileChunkShift, AtomicLong availableSpace)
    {
      super(file);
      this.availableSpace = availableSpace;
      this.indexFileChunkShift = indexFileChunkShift;
      this.dataFileChunkShift = dataFileChunkShift;
    }

    @Override
    protected Volume.Factory extendStoreVolumeFactory() {
      final File indexFile = new File(props.getProperty(Keys.file));
      final File dataFile = new File(indexFile.getPath() + StoreDirect.DATA_FILE_EXT);

      return new Volume.Factory() {
        @Override
        public Volume createIndexVolume() {
          return new MappedFileVolWrapper(indexFile, indexFileChunkShift, availableSpace);
        }

        @Override
        public Volume createPhysVolume() {
          return new MappedFileVolWrapper(dataFile, dataFileChunkShift, availableSpace);
        }

        @Override
        public Volume createTransLogVolume() {
          throw new UnsupportedOperationException("not supported");
        }
      };
    }
  }
}
