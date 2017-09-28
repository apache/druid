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

package io.druid.data.input.impl.prefetch;

import org.apache.commons.io.FileUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * A class containing meta information about opened object.  This class is used by
 * {@link PrefetchableTextFilesFirehoseFactory}.
 */
class OpenedObject<ObjectType> implements Closeable
{
  // Original object
  private final ObjectType object;
  // Input stream from the object
  private final InputStream objectStream;
  // Closer which is called when the file is not needed anymore. Usually this deletes the file except for cached files.
  private final Closeable resourceCloser;

  OpenedObject(FetchedFile<ObjectType> fetchedFile) throws IOException
  {
    this(fetchedFile.getObject(), FileUtils.openInputStream(fetchedFile.getFile()), fetchedFile.getResourceCloser());
  }

  OpenedObject(ObjectType object, InputStream objectStream, Closeable resourceCloser)
  {
    this.object = object;
    this.objectStream = objectStream;
    this.resourceCloser = resourceCloser;
  }

  ObjectType getObject()
  {
    return object;
  }

  InputStream getObjectStream()
  {
    return objectStream;
  }

  @Override
  public void close() throws IOException
  {
    resourceCloser.close();
  }
}
