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

package org.apache.druid.data.input;

import org.apache.druid.data.input.impl.InputEntityIteratingReader;
import org.apache.druid.data.input.impl.systemfield.SystemFieldDecoratorFactory;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.utils.CompressionUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;

/**
 * {@link InputSource} backed by {@link ClassLoader#getResourceAsStream(String)}, for all your embedded test data
 * ingestion needs
 */
public class ResourceInputSource extends AbstractInputSource
{
  public static ResourceInputSource of(ClassLoader loader, String resourceFile)
  {
    return new ResourceInputSource(loader, resourceFile);
  }

  private final ClassLoader classLoader;
  private final String resourceFile;

  private ResourceInputSource(ClassLoader classLoader, String resourceFile)
  {
    this.classLoader = classLoader;
    this.resourceFile = resourceFile;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public boolean needsFormat()
  {
    return true;
  }

  @Override
  protected InputSourceReader formattableReader(
      InputRowSchema inputRowSchema,
      InputFormat inputFormat,
      File temporaryDirectory
  )
  {
    return new InputEntityIteratingReader(
        inputRowSchema,
        inputFormat,
        CloseableIterators.withEmptyBaggage(
            Collections.singletonList(new ResourceStreamEntity(classLoader, resourceFile)).iterator()
        ),
        SystemFieldDecoratorFactory.NONE,
        temporaryDirectory
    );
  }

  public static class ResourceStreamEntity implements InputEntity
  {
    private final ClassLoader classLoader;
    private final String resourceFile;

    public ResourceStreamEntity(ClassLoader classLoader, String resourceFile)
    {
      this.classLoader = classLoader;
      this.resourceFile = resourceFile;
    }

    @Nullable
    @Override
    public URI getUri()
    {
      try {
        return classLoader.getResource(resourceFile).toURI();
      }
      catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public InputStream open() throws IOException
    {
      final InputStream resourceStream = classLoader.getResourceAsStream(resourceFile);
      return CompressionUtils.decompress(resourceStream, resourceFile);
    }
  }
}
