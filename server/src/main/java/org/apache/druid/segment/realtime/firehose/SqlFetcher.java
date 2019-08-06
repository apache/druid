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

package org.apache.druid.segment.realtime.firehose;

import org.apache.druid.data.input.impl.prefetch.CacheManager;
import org.apache.druid.data.input.impl.prefetch.Fetcher;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.data.input.impl.prefetch.OpenedObject;
import org.apache.druid.data.input.impl.prefetch.PrefetchConfig;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;


/**
 * A file fetcher used by {@link PrefetchSqlFirehoseFactory}.
 * See the javadoc of {@link PrefetchSqlFirehoseFactory} for more details.
 */
public class SqlFetcher<T> extends Fetcher<T>
{
  private static final String FETCH_FILE_PREFIX = "sqlfetch-";

  @Nullable
  private final File temporaryDirectory;

  private final ObjectOpenFunction<T> openObjectFunction;

  SqlFetcher(
      CacheManager<T> cacheManager,
      List<T> objects,
      ExecutorService fetchExecutor,
      @Nullable File temporaryDirectory,
      PrefetchConfig prefetchConfig,
      ObjectOpenFunction<T> openObjectFunction
  )
  {

    super(
        cacheManager,
        objects,
        fetchExecutor,
        temporaryDirectory,
        prefetchConfig
    );
    this.temporaryDirectory = temporaryDirectory;
    this.openObjectFunction = openObjectFunction;
  }

  /**
   * Downloads the entire resultset object into a file. This avoids maintaining a
   * persistent connection to the database. The retry is performed at the query execution layer.
   *
   * @param object  sql query for which the resultset is to be downloaded
   * @param outFile a file which the object data is stored
   *
   * @return size of downloaded resultset
   */

  @Override
  protected long download(T object, File outFile) throws IOException
  {
    openObjectFunction.open(object, outFile);
    return outFile.length();
  }

  /**
   * Generates an instance of {@link OpenedObject} for the given object. This is usually called
   * when prefetching is disabled. The retry is performed at the query execution layer.
   */

  @Override
  protected OpenedObject<T> generateOpenObject(T object) throws IOException
  {
    final File outFile = File.createTempFile(FETCH_FILE_PREFIX, null, temporaryDirectory);
    return new OpenedObject<>(
        object,
        openObjectFunction.open(object, outFile),
        outFile::delete
    );
  }
}
