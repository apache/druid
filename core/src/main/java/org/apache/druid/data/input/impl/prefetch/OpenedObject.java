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

package org.apache.druid.data.input.impl.prefetch;

import org.apache.commons.io.FileUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * A class containing meta information about an opened object.  This class is used to put related objects together.  It
 * contains an original object, an objectStream from the object, and a resourceCloser which knows how to release
 * associated resources on closing.
 *
 * {@link PrefetchableTextFilesFirehoseFactory.ResourceCloseableLineIterator} consumes the objectStream and closes
 * it with the resourceCloser.
 */
public class OpenedObject<T>
{
  // Original object
  private final T object;
  // Input stream from the object
  private final InputStream objectStream;
  // Closer which is called when the file is not needed anymore. Usually this deletes the file except for cached files.
  private final Closeable resourceCloser;

  public OpenedObject(FetchedFile<T> fetchedFile) throws IOException
  {
    this(fetchedFile.getObject(), FileUtils.openInputStream(fetchedFile.getFile()), fetchedFile.getResourceCloser());
  }

  public OpenedObject(T object, InputStream objectStream, Closeable resourceCloser)
  {
    this.object = object;
    this.objectStream = objectStream;
    this.resourceCloser = resourceCloser;
  }

  public T getObject()
  {
    return object;
  }

  public InputStream getObjectStream()
  {
    return objectStream;
  }

  public Closeable getResourceCloser()
  {
    return resourceCloser;
  }
}
