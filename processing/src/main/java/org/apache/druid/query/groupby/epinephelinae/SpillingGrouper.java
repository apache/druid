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
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.segment.ColumnSelectorFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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

  private final Grouper<KeyType> grouper;
  private final KeySerde<KeyType> keySerde;
  private final LimitedTemporaryStorage temporaryStorage;
  private final ObjectMapper spillMapper;
  private final AggregatorFactory[] aggregatorFactories;
  private final Comparator<Grouper.Entry<KeyType>> keyObjComparator;
  private final Comparator<Grouper.Entry<KeyType>> defaultOrderKeyObjComparator;

  private final List<File> files = new ArrayList<>();
  private final List<File> dictionaryFiles = new ArrayList<>();
  private final boolean sortHasNonGroupingFields;

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
      final int mergeBufferSize
  )
  {
    this.keySerde = keySerdeFactory.factorize();
    this.keyObjComparator = keySerdeFactory.objectComparator(false);
    this.defaultOrderKeyObjComparator = keySerdeFactory.objectComparator(true);
    if (limitSpec != null) {
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
    this.spillMapper = spillMapper;
    this.spillingAllowed = spillingAllowed;
    this.sortHasNonGroupingFields = sortHasNonGroupingFields;
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
        return DISK_FULL;
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
    deleteFiles();
  }

  @Override
  public void close()
  {
    grouper.close();
    deleteFiles();
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

  public void setSpillingAllowed(final boolean spillingAllowed)
  {
    this.spillingAllowed = spillingAllowed;
  }

  @Override
  public CloseableIterator<Entry<KeyType>> iterator(final boolean sorted)
  {
    final List<CloseableIterator<Entry<KeyType>>> iterators = new ArrayList<>(1 + files.size());

    iterators.add(grouper.iterator(sorted));

    final Closer closer = Closer.create();
    for (final File file : files) {
      final MappingIterator<Entry<KeyType>> fileIterator = read(file, keySerde.keyClazz());
      iterators.add(
          CloseableIterators.withEmptyBaggage(
              Iterators.transform(
                  fileIterator,
                  new Function<Entry<KeyType>, Entry<KeyType>>()
                  {
                    @Override
                    public Entry<KeyType> apply(Entry<KeyType> entry)
                    {
                      final Object[] deserializedValues = new Object[entry.getValues().length];
                      for (int i = 0; i < deserializedValues.length; i++) {
                        deserializedValues[i] = aggregatorFactories[i].deserialize(entry.getValues()[i]);
                        if (deserializedValues[i] instanceof Integer) {
                          // Hack to satisfy the groupBy unit tests; perhaps we could do better by adjusting Jackson config.
                          deserializedValues[i] = ((Integer) deserializedValues[i]).longValue();
                        }
                      }
                      return new Entry<>(entry.getKey(), deserializedValues);
                    }
                  }
              )
          )
      );
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
    try (CloseableIterator<Entry<KeyType>> iterator = grouper.iterator(true)) {
      files.add(spill(iterator));
      dictionaryFiles.add(spill(keySerde.getDictionary().iterator()));

      grouper.reset();
    }
  }

  private <T> File spill(Iterator<T> iterator) throws IOException
  {
    try (
        final LimitedTemporaryStorage.LimitedOutputStream out = temporaryStorage.createFile();
        final LZ4BlockOutputStream compressedOut = new LZ4BlockOutputStream(out);
        final JsonGenerator jsonGenerator = spillMapper.getFactory().createGenerator(compressedOut)
    ) {
      while (iterator.hasNext()) {
        BaseQuery.checkInterrupted();

        jsonGenerator.writeObject(iterator.next());
      }

      return out.getFile();
    }
  }

  private MappingIterator<Entry<KeyType>> read(final File file, final Class<KeyType> keyClazz)
  {
    try {
      return spillMapper.readValues(
          spillMapper.getFactory().createParser(new LZ4BlockInputStream(new FileInputStream(file))),
          spillMapper.getTypeFactory().constructParametricType(Entry.class, keyClazz)
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
  }
}
