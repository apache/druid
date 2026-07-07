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

package org.apache.druid.query.groupby.epinephelinae;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByStatsProvider;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.segment.ColumnSelectorFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Grouper based around a single underlying {@link BufferHashGrouper}. Not thread-safe.
 *
 * When the underlying grouper is full, its contents are sorted and written to temporary files using "spillMapper".
 */
public class SpillingGrouper<KeyType> implements Grouper<KeyType>
{
  private static final Logger log = new Logger(SpillingGrouper.class);
  private static final AggregateResult DISK_FULL = AggregateResult.partial(
      0,
      "Not enough disk space to execute this query. Try raising druid.query.groupBy.maxOnDiskStorage."
  );
  private static final AggregateResult MAX_FILE = AggregateResult.partial(
      0,
      "Maximum number of spill files reached for this query. Try raising druid.query.groupBy.maxSpillFileCount."
  );

  private final AbstractBufferHashGrouper<KeyType> grouper;
  private final KeySerde<KeyType> keySerde;
  private final LimitedTemporaryStorage temporaryStorage;
  private final ObjectMapper spillMapper;
  private final AggregatorFactory[] aggregatorFactories;
  private final Comparator<Grouper.Entry<KeyType>> keyObjComparator;
  private final Comparator<Grouper.Entry<KeyType>> defaultOrderKeyObjComparator;
  private final GroupByStatsProvider.PerQueryStats perQueryStats;
  private final long minSpillFileSize;

  private final List<File> files = new ArrayList<>();
  private final List<File> dictionaryFiles = new ArrayList<>();
  private final boolean sortHasNonGroupingFields;

  // Pending spill runs not yet written to disk. Each entry is one buffer flush serialized as a
  // LZ4-compressed JSON byte array — the same format as an on-disk spill file, so it can be
  // re-read with the same read() path. Runs are held in heap memory and merged into a single
  // sorted file only when pendingSpillBytes reaches minSpillFileSize.
  private final List<byte[]> pendingSpillRuns = new ArrayList<>();
  private final Set<String> pendingDictionaryEntries = new HashSet<>();
  private long pendingSpillBytes = 0;

  private boolean diskFull = false;
  private boolean maxFileCount = false;
  private boolean spillingAllowed;

  public SpillingGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      final KeySerdeFactory<KeyType> keySerdeFactory,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final int bufferGrouperMaxSize,
      final float bufferGrouperMaxLoadFactor,
      final int bufferGrouperInitialBuckets,
      final LimitedTemporaryStorage temporaryStorage,
      final ObjectMapper spillMapper,
      final boolean spillingAllowed,
      final DefaultLimitSpec limitSpec,
      final boolean sortHasNonGroupingFields,
      final int mergeBufferSize,
      final long minSpillFileSize,
      final GroupByStatsProvider.PerQueryStats perQueryStats
  )
  {
    this.keySerde = keySerdeFactory.factorize();
    this.keyObjComparator = keySerdeFactory.objectComparator(false);
    this.defaultOrderKeyObjComparator = keySerdeFactory.objectComparator(true);
    if (limitSpec != null) {
      // Sanity check; must not have "offset" at this point.
      Preconditions.checkState(!limitSpec.isOffset(), "Cannot push down offsets");

      LimitedBufferHashGrouper<KeyType> limitGrouper = new LimitedBufferHashGrouper<>(
          bufferSupplier,
          keySerde,
          AggregatorAdapters.factorizeBuffered(columnSelectorFactory, Arrays.asList(aggregatorFactories)),
          bufferGrouperMaxSize,
          bufferGrouperMaxLoadFactor,
          bufferGrouperInitialBuckets,
          limitSpec.getLimit(),
          sortHasNonGroupingFields
      );
      // if configured buffer size is too small to support limit push down, don't apply that optimization
      if (!limitGrouper.validateBufferCapacity(mergeBufferSize)) {
        if (sortHasNonGroupingFields) {
          log.debug("Ignoring forceLimitPushDown, insufficient buffer capacity.");
        }
        // sortHasNonGroupingFields can only be true here if the user specified forceLimitPushDown
        // in the query context. Result merging requires that all results are sorted by the same
        // ordering where all ordering fields are contained in the grouping key.
        // If sortHasNonGroupingFields is true, we use the default ordering that sorts by all grouping key fields
        // with lexicographic ascending order.
        // If sortHasNonGroupingFields is false, then the OrderBy fields are all in the grouping key, so we
        // can use that ordering.
        this.grouper = new BufferHashGrouper<>(
            bufferSupplier,
            keySerde,
            AggregatorAdapters.factorizeBuffered(columnSelectorFactory, Arrays.asList(aggregatorFactories)),
            bufferGrouperMaxSize,
            bufferGrouperMaxLoadFactor,
            bufferGrouperInitialBuckets,
            sortHasNonGroupingFields
        );
      } else {
        this.grouper = limitGrouper;
      }
    } else {
      this.grouper = new BufferHashGrouper<>(
          bufferSupplier,
          keySerde,
          AggregatorAdapters.factorizeBuffered(columnSelectorFactory, Arrays.asList(aggregatorFactories)),
          bufferGrouperMaxSize,
          bufferGrouperMaxLoadFactor,
          bufferGrouperInitialBuckets,
          true
      );
    }
    this.aggregatorFactories = aggregatorFactories;
    this.temporaryStorage = temporaryStorage;
    this.spillMapper = keySerde.decorateObjectMapper(spillMapper);
    this.spillingAllowed = spillingAllowed;
    this.sortHasNonGroupingFields = sortHasNonGroupingFields;
    this.minSpillFileSize = minSpillFileSize;
    this.perQueryStats = perQueryStats;
  }

  @Override
  public void init()
  {
    grouper.init();
  }

  @Override
  public boolean isInitialized()
  {
    return grouper.isInitialized();
  }

  @Override
  public AggregateResult aggregate(KeyType key, int keyHash)
  {
    if (diskFull) {
      // If the prior return was DISK_FULL, then return it again. When we return DISK_FULL to a processing thread,
      // it skips the rest of the segment and the query is canceled. However, it's possible that the next segment
      // starts processing before cancellation can kick in. We want that one, if it occurs, to see DISK_FULL too.
      return DISK_FULL;
    }

    if (maxFileCount) {
      return MAX_FILE;
    }

    final AggregateResult result = grouper.aggregate(key, keyHash);

    if (result.isOk() || !spillingAllowed || temporaryStorage.maxSize() <= 0) {
      return result;
    } else {
      // Expecting all-or-nothing behavior.
      assert result.getCount() == 0;

      // Warning: this can potentially block up a processing thread for a while.
      try {
        spill();
      }
      catch (TemporaryStorageFullException e) {
        diskFull = true;
        return DISK_FULL;
      }
      catch (TemporaryStorageFileLimitException e) {
        maxFileCount = true;
        return MAX_FILE;
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }

      // Try again.
      return grouper.aggregate(key, keyHash);
    }
  }

  @Override
  public void reset()
  {
    grouper.reset();
    pendingSpillRuns.clear();
    pendingSpillBytes = 0;
    pendingDictionaryEntries.clear();
    deleteFiles();
  }

  @Override
  public void close()
  {
    perQueryStats.dictionarySize(getDictionarySizeEstimate());
    final long sliceUsedBytes = getMaxMergeBufferUsedBytes();
    perQueryStats.addMergeBufferUsedBytes(sliceUsedBytes);
    if (grouper.isInitialized()) {
      // Report this slice's peak fill ratio (bucket-count based, tracked by the underlying hash table across the
      // grouper's lifetime and preserved across reset()). 1.0 means the grouper actually hit its spill trigger; a
      // lightly-filled slice reports <1.0 exactly. Only recorded when the grouper was initialized, so an untouched
      // slice contributes nothing.
      perQueryStats.sliceUsage(grouper.getMaxSpillProximity());
    }
    // Record spilled bytes before deleteFiles() decrements bytesUsed in temporaryStorage.
    long spilledBytes = 0;
    for (final File file : files) {
      spilledBytes += file.length();
    }
    for (final File file : dictionaryFiles) {
      spilledBytes += file.length();
    }
    perQueryStats.spilledBytes(spilledBytes);
    grouper.close();
    keySerde.reset();
    pendingSpillRuns.clear();
    pendingSpillBytes = 0;
    pendingDictionaryEntries.clear();
    deleteFiles();
  }

  private long getMaxMergeBufferUsedBytes()
  {
    return grouper.isInitialized() ? grouper.getMaxMergeBufferUsedBytes() : 0L;
  }

  private long getDictionarySizeEstimate()
  {
    return keySerde.getDictionarySize();
  }

  /**
   * Returns a dictionary of string keys added to this grouper.  Note that the dictionary of keySerde is spilled on
   * local storage whenever the inner grouper is spilled.  If there are spilled dictionaries, this method loads them
   * from disk and returns a merged dictionary.
   *
   * @return a dictionary which is a list of unique strings
   */
  public List<String> mergeAndGetDictionary()
  {
    final Set<String> mergedDictionary = new HashSet<>(keySerde.getDictionary());

    for (File dictFile : dictionaryFiles) {
      try (
          final InputStream fileStream = Files.newInputStream(dictFile.toPath());
          final LZ4BlockInputStream blockStream = new LZ4BlockInputStream(fileStream);
          final MappingIterator<String> dictIterator = spillMapper.readValues(
              spillMapper.getFactory().createParser(blockStream),
              spillMapper.getTypeFactory().constructType(String.class)
          )
      ) {
        while (dictIterator.hasNext()) {
          mergedDictionary.add(dictIterator.next());
        }
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    return new ArrayList<>(mergedDictionary);
  }

  public boolean isSpillingAllowed()
  {
    return spillingAllowed;
  }

  public void setSpillingAllowed(final boolean spillingAllowed)
  {
    this.spillingAllowed = spillingAllowed;
  }

  /**
   * Returns an iterator over all grouped entries, merging results from the in-memory grouper and
   * any spill files on disk. When sorted is true, uses a merge-sorted iterator across all sources;
   * when false, simply concatenates them.
   *
   * <p>In practice, sorted is always true. {@link RowBasedGrouperHelper} hardcodes
   * {@code grouper.iterator(true)} because the merge layer above — CombiningIterator in
   * {@link ConcurrentGrouper} and the broker merge — detects duplicate keys by comparing
   * consecutive sorted entries. So sorted=true is required for merge correctness, not output
   * ordering. The sorted=false path exists but is unreachable through any production query path.
   */
  @Override
  public CloseableIterator<Entry<KeyType>> iterator(final boolean sorted)
  {
    // Flush any runs that did not reach minSpillFileSize during the spill phase.
    try {
      flushPendingRunsToDisk();
    }
    catch (TemporaryStorageFullException e) {
      diskFull = true;
      throw new ResourceLimitExceededException(DISK_FULL.getReason());
    }
    catch (TemporaryStorageFileLimitException e) {
      maxFileCount = true;
      throw new ResourceLimitExceededException(MAX_FILE.getReason());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    final List<CloseableIterator<Entry<KeyType>>> iterators = new ArrayList<>(1 + files.size());

    iterators.add(grouper.iterator(sorted));

    final Closer closer = Closer.create();
    for (final File file : files) {
      final MappingIterator<Entry<KeyType>> fileIterator = read(file, keySerde.keyClazz());

      iterators.add(deserializeIterator(fileIterator));
      closer.register(fileIterator);
    }

    final Iterator<Entry<KeyType>> baseIterator;
    if (sortHasNonGroupingFields) {
      baseIterator = CloseableIterators.mergeSorted(iterators, defaultOrderKeyObjComparator);
    } else {
      baseIterator = sorted ?
                     CloseableIterators.mergeSorted(iterators, keyObjComparator) :
                     CloseableIterators.concat(iterators);
    }

    return CloseableIterators.wrap(baseIterator, closer);
  }

  private void spill() throws IOException
  {
    final SpillOutputStream spillOut = new SpillOutputStream(temporaryStorage, minSpillFileSize);
    try (CloseableIterator<Entry<KeyType>> iterator = grouper.iterator(true)) {
      serializeToStream(iterator, spillOut);
    }

    pendingDictionaryEntries.addAll(keySerde.getDictionary());
    grouper.reset();

    if (spillOut.isInMemory()) {
      final byte[] bytes = spillOut.toByteArray();
      pendingSpillRuns.add(bytes);
      pendingSpillBytes += bytes.length;

      if (pendingSpillBytes >= minSpillFileSize) {
        flushPendingRunsToDisk();
      }
    } else {
      files.add(spillOut.getFile());
      dictionaryFiles.add(spill(pendingDictionaryEntries.iterator()));
      pendingDictionaryEntries.clear();
    }
  }

  /**
   * Merge-sorts all pending in-memory spill runs and writes them as a single sorted file to disk.
   * Each run is already individually sorted (from grouper.iterator(true)); this method merges them
   * so the output file is fully sorted, as required by iterator()'s mergeSorted across files.
   * <p>
   * We always merge-sort rather than concatenating runs (regardless of sorted / sortHasNonGroupingFields flags).
   * The processing cost is dominated by JSON deserialization and re-serialization; the merge-sort comparison itself
   * is O(N log K) key comparisons and negligible relative to the serde overhead, so concatenation would save little.
   * <p>
   * An alternative approach of writing each pending run's raw byte[] sequentially into one file
   * (avoiding serde entirely) was rejected because at read time each sub-stream would require its own
   * LZ4BlockInputStream with an internal buffer. With large amount of small spills we can end up with large number of
   * sub-streams, each with its own buffer, which can lead to OOM. By merging runs together, we ensure that the number
   * of spill files (and thus sub-streams) is small regardless of spill pattern.
   */
  private void flushPendingRunsToDisk() throws IOException
  {
    if (pendingSpillRuns.isEmpty()) {
      return;
    }

    final Comparator<Entry<KeyType>> sortComparator =
        sortHasNonGroupingFields ? defaultOrderKeyObjComparator : keyObjComparator;

    final List<MappingIterator<Entry<KeyType>>> readers = new ArrayList<>(pendingSpillRuns.size());
    try {
      for (final byte[] runBytes : pendingSpillRuns) {
        readers.add(spillMapper.readValues(
            spillMapper.getFactory().createParser(new LZ4BlockInputStream(new ByteArrayInputStream(runBytes))),
            spillMapper.getTypeFactory().constructParametricType(ReusableEntry.class, keySerde.keyClazz())
        ));
      }
      final List<CloseableIterator<Entry<KeyType>>> iterators = new ArrayList<>(readers.size());
      for (final MappingIterator<Entry<KeyType>> reader : readers) {
        iterators.add(deserializeIterator(reader));
      }
      files.add(spill(CloseableIterators.mergeSorted(iterators, sortComparator)));
      dictionaryFiles.add(spill(pendingDictionaryEntries.iterator()));
    }
    finally {
      for (final MappingIterator<Entry<KeyType>> reader : readers) {
        try {
          reader.close();
        }
        catch (IOException e) {
          log.warn(e, "Failed to close reader while flushing pending spill runs");
        }
      }
      pendingSpillRuns.clear();
      pendingSpillBytes = 0;
      pendingDictionaryEntries.clear();
    }
  }

  private CloseableIterator<Entry<KeyType>> deserializeIterator(final Iterator<Entry<KeyType>> iterator)
  {
    return CloseableIterators.withEmptyBaggage(
        Iterators.transform(
            iterator,
            new Function<>()
            {
              final ReusableEntry<KeyType> reusableEntry =
                  ReusableEntry.create(keySerde, aggregatorFactories.length);

              @Override
              public Entry<KeyType> apply(Entry<KeyType> entry)
              {
                final Object[] deserializedValues = reusableEntry.getValues();
                for (int i = 0; i < deserializedValues.length; i++) {
                  deserializedValues[i] = aggregatorFactories[i].deserialize(entry.getValues()[i]);
                  if (deserializedValues[i] instanceof Integer) {
                    deserializedValues[i] = ((Integer) deserializedValues[i]).longValue();
                  }
                }
                reusableEntry.setKey(entry.getKey());
                return reusableEntry;
              }
            }
        )
    );
  }

  private <T> void serializeToStream(Iterator<T> iterator, OutputStream out) throws IOException
  {
    try (
        final LZ4BlockOutputStream compressedOut = new LZ4BlockOutputStream(out);
        final JsonGenerator jsonGenerator = spillMapper.getFactory().createGenerator(compressedOut)
    ) {
      final SerializerProvider serializers = spillMapper.getSerializerProviderInstance();
      while (iterator.hasNext()) {
        BaseQuery.checkInterrupted();
        JacksonUtils.writeObjectUsingSerializerProvider(jsonGenerator, serializers, iterator.next());
      }
    }
  }

  private <T> File spill(Iterator<T> iterator) throws IOException
  {
    try (final LimitedTemporaryStorage.LimitedOutputStream out = temporaryStorage.createFile()) {
      serializeToStream(iterator, out);
      return out.getFile();
    }
  }

  private MappingIterator<Entry<KeyType>> read(final File file, final Class<KeyType> keyClazz)
  {
    try {
      return spillMapper.readValues(
          spillMapper.getFactory().createParser(new LZ4BlockInputStream(new FileInputStream(file))),
          spillMapper.getTypeFactory().constructParametricType(ReusableEntry.class, keyClazz)
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void deleteFiles()
  {
    for (final File file : files) {
      temporaryStorage.delete(file);
    }
    files.clear();
    for (final File file : dictionaryFiles) {
      temporaryStorage.delete(file);
    }
    dictionaryFiles.clear();
  }
}
