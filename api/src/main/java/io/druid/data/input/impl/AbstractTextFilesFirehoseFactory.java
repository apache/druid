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

  @Override
  public Firehose connect(StringInputRowParser firehoseParser) throws IOException
  {
    final List<ObjectType> objects = ImmutableList.copyOf(Preconditions.checkNotNull(initObjects(), "initObjects"));
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
              return IOUtils.lineIterator(
                  new BufferedReader(
                      new InputStreamReader(openStream(object), Charsets.UTF_8)
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

  /**
   * Initialize objects to be read by this firehose.  Since firehose factories are constructed whenever
   * io.druid.indexing.common.task.Task objects are deserialized, actual initialization of objects is deferred
   * until {@link #connect(StringInputRowParser)} is called.
   *
   * @return a collection of initialized objects.
   */
  protected abstract Collection<ObjectType> initObjects();

  /**
   * Open an input stream from the given object.
   *
   * @param object an object to be read
   *
   * @return an input stream for the object
   *
   * @throws IOException
   */
  protected abstract InputStream openStream(ObjectType object) throws IOException;
}
