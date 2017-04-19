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

package io.druid.data.input.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.logger.Logger;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * This is an abstract class for firehose factory for making firehoses reading text files.
 * It provides an unified {@link #connect(StringInputRowParser)} implementation for its subclasses.
 *
 * @param <ObjectType> object type representing input data
 */
public abstract class AbstractTextFilesFirehoseFactory<ObjectType>
    implements FirehoseFactory<StringInputRowParser>
{
  private static final Logger LOG = new Logger(AbstractTextFilesFirehoseFactory.class);

  private final List<ObjectType> objects;

  public AbstractTextFilesFirehoseFactory(Collection<ObjectType> objects)
  {
    this.objects = ImmutableList.copyOf(Preconditions.checkNotNull(objects));
  }

  @Override
  public Firehose connect(StringInputRowParser firehoseParser) throws IOException
  {
    final Iterator<ObjectType> iterator = objects.iterator();
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
            final ObjectType object = iterator.next();
            try {
              final InputStream stream = openStream(object);
              return IOUtils.lineIterator(
                  new BufferedReader(
                      new InputStreamReader(wrapIfNeeded(object, stream), Charsets.UTF_8)
                  )
              );
            }
            catch (Exception e) {
              LOG.error(
                  e,
                  "Exception reading object[%s]",
                  object
              );
              throw Throwables.propagate(e);
            }
          }
        },
        firehoseParser
    );
  }

  public List<ObjectType> getObjects()
  {
    return objects;
  }

  /**
   * Wrap the given stream if needed, currently when the given object is compressed with gzip.
   *
   * @param object an object
   * @param innerStream input stream for object
   *
   * @return wrapped input stream if the object is gzipped
   *
   * @throws IOException
   */
  protected InputStream wrapIfNeeded(ObjectType object, InputStream innerStream) throws IOException
  {
    return wrapIfNeeded(innerStream, isGzipped(object));
  }

  protected static InputStream wrapIfNeeded(InputStream innerStream, boolean isGzipped) throws IOException
  {
    if (isGzipped) {
      return CompressionUtils.gzipInputStream(innerStream);
    }

    return innerStream;
  }

  /**
   * Open an input stream from the given object.
   * The result input stream must not be wrapped even if the object is compressed with gzip.
   *
   * @param object an object to be read
   *
   * @return an input stream for the object
   *
   * @throws IOException
   */
  protected abstract InputStream openStream(ObjectType object) throws IOException;

  /**
   * Check if the object is compressed with gzip.
   *
   * @param object an object to be checked
   *
   * @return true if the object is compressed with gzip
   */
  protected abstract boolean isGzipped(ObjectType object);
}
