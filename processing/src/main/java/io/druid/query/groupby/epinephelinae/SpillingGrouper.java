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

package io.druid.query.groupby.epinephelinae;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.QueryInterruptedException;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.ColumnSelectorFactory;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Grouper based around a single underlying {@link BufferGrouper}. Not thread-safe.
 *
 * When the underlying grouper is full, its contents are sorted and written to temporary files using "spillMapper".
 */
public class SpillingGrouper<KeyType extends Comparable<KeyType>> implements Grouper<KeyType>
{
  private static final Logger log = new Logger(SpillingGrouper.class);

  private final BufferGrouper<KeyType> grouper;
  private final KeySerde<KeyType> keySerde;
  private final LimitedTemporaryStorage temporaryStorage;
  private final ObjectMapper spillMapper;
  private final AggregatorFactory[] aggregatorFactories;

  private final List<File> files = Lists.newArrayList();
  private final List<Closeable> closeables = Lists.newArrayList();

  private boolean spillingAllowed = false;

  public SpillingGrouper(
      final ByteBuffer buffer,
      final KeySerdeFactory<KeyType> keySerdeFactory,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final int bufferGrouperMaxSize,
      final float bufferGrouperMaxLoadFactor,
      final int bufferGrouperInitialBuckets,
      final LimitedTemporaryStorage temporaryStorage,
      final ObjectMapper spillMapper,
      final boolean spillingAllowed
  )
  {
    this.keySerde = keySerdeFactory.factorize();
    this.grouper = new BufferGrouper<>(
        buffer,
        keySerde,
        columnSelectorFactory,
        aggregatorFactories,
        bufferGrouperMaxSize,
        bufferGrouperMaxLoadFactor,
        bufferGrouperInitialBuckets
    );
    this.aggregatorFactories = aggregatorFactories;
    this.temporaryStorage = temporaryStorage;
    this.spillMapper = spillMapper;
    this.spillingAllowed = spillingAllowed;
  }

  @Override
  public boolean aggregate(KeyType key, int keyHash)
  {
    if (grouper.aggregate(key, keyHash)) {
      return true;
    } else if (spillingAllowed) {
      // Warning: this can potentially block up a processing thread for a while.
      try {
        spill();
      }
      catch (TemporaryStorageFullException e) {
        return false;
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
      return grouper.aggregate(key, keyHash);
    } else {
      return false;
    }
  }

  @Override
  public boolean aggregate(KeyType key)
  {
    return aggregate(key, Groupers.hash(key));
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

  public void setSpillingAllowed(final boolean spillingAllowed)
  {
    this.spillingAllowed = spillingAllowed;
  }

  @Override
  public Iterator<Entry<KeyType>> iterator(final boolean sorted)
  {
    final List<Iterator<Entry<KeyType>>> iterators = new ArrayList<>(1 + files.size());

    iterators.add(grouper.iterator(sorted));

    for (final File file : files) {
      final MappingIterator<Entry<KeyType>> fileIterator = read(file, keySerde.keyClazz());
      iterators.add(
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
      );
      closeables.add(fileIterator);
    }

    return Groupers.mergeIterators(iterators, sorted);
  }

  private void spill() throws IOException
  {
    final File outFile;

    try (
        final LimitedTemporaryStorage.LimitedOutputStream out = temporaryStorage.createFile();
        final LZ4BlockOutputStream compressedOut = new LZ4BlockOutputStream(out);
        final JsonGenerator jsonGenerator = spillMapper.getFactory().createGenerator(compressedOut)
    ) {
      outFile = out.getFile();
      final Iterator<Entry<KeyType>> it = grouper.iterator(true);
      while (it.hasNext()) {
        if (Thread.interrupted()) {
          throw new QueryInterruptedException(new InterruptedException());
        }

        jsonGenerator.writeObject(it.next());
      }
    }

    files.add(outFile);
    grouper.reset();
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
      throw Throwables.propagate(e);
    }
  }

  private void deleteFiles()
  {
    for (Closeable closeable : closeables) {
      // CloseQuietly is OK on readable streams
      CloseQuietly.close(closeable);
    }
    for (final File file : files) {
      temporaryStorage.delete(file);
    }
    files.clear();
  }
}
