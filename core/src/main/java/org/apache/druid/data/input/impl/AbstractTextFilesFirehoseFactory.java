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

package org.apache.druid.data.input.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * This is an abstract class for firehose factory for making firehoses reading text files.
 * It provides an unified {@link #connect(StringInputRowParser, File)} implementation for its subclasses.
 *
 * @param <T> object type representing input data
 */
public abstract class AbstractTextFilesFirehoseFactory<T>
    implements FiniteFirehoseFactory<StringInputRowParser, T>
{
  private static final Logger LOG = new Logger(AbstractTextFilesFirehoseFactory.class);

  private List<T> objects;

  @Override
  public Firehose connect(StringInputRowParser firehoseParser, File temporaryDirectory) throws IOException
  {
    initializeObjectsIfNeeded();
    final Iterator<T> iterator = objects.iterator();
    return new FileIteratingFirehose(
        new Iterator<LineIterator>()
        {
          @Override
          public boolean hasNext()
          {
            return iterator.hasNext();
          }

          @Override
          public LineIterator next()
          {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            final T object = iterator.next();
            try {
              return IOUtils.lineIterator(wrapObjectStream(object, openObjectStream(object)), StandardCharsets.UTF_8);
            }
            catch (Exception e) {
              LOG.error(e, "Exception reading object[%s]", object);
              throw new RuntimeException(e);
            }
          }
        },
        firehoseParser
    );
  }

  protected void initializeObjectsIfNeeded() throws IOException
  {
    if (objects == null) {
      objects = ImmutableList.copyOf(Preconditions.checkNotNull(initObjects(), "initObjects"));
    }
  }

  public List<T> getObjects()
  {
    return objects;
  }

  @Override
  public Stream<InputSplit<T>> getSplits(@Nullable SplitHintSpec splitHintSpec) throws IOException
  {
    initializeObjectsIfNeeded();
    return getObjects().stream().map(InputSplit::new);
  }

  @Override
  public int getNumSplits(@Nullable SplitHintSpec splitHintSpec) throws IOException
  {
    initializeObjectsIfNeeded();
    return getObjects().size();
  }

  /**
   * Initialize objects to be read by this firehose.  Since firehose factories are constructed whenever
   * org.apache.druid.indexing.common.task.Task objects are deserialized, actual initialization of objects is deferred
   * until {@link #connect(StringInputRowParser, File)} is called.
   *
   * @return a collection of initialized objects.
   */
  protected abstract Collection<T> initObjects() throws IOException;

  /**
   * Open an input stream from the given object.  If the object is compressed, this method should return a byte stream
   * as it is compressed.  The object compression should be handled in {@link #wrapObjectStream(Object, InputStream)}.
   *
   * @param object an object to be read
   *
   * @return an input stream for the object
   */
  protected abstract InputStream openObjectStream(T object) throws IOException;

  /**
   * Wrap the given input stream if needed.  The decompression logic should be applied to the given stream if the object
   * is compressed.
   *
   * @param object an input object
   * @param stream a stream for the object
   * @return an wrapped input stream
   */
  protected abstract InputStream wrapObjectStream(T object, InputStream stream) throws IOException;
}
