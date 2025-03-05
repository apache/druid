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

package org.apache.druid.storage.local;

import com.google.inject.Provider;
import org.apache.druid.java.util.common.FileUtils;

import java.io.File;

/**
 * {@link LocalTmpStorageConfig} is a provider for temporary directories. A default implementation is binded in all services except
 * Peon. For peons, a custom implementation is binded in CliPeon which uses the working directory of the peon to create
 * a temporary storage. This interface will be guice injectable in all services.
 * The cleaning up of the temporary files/directories created in this storage is handled by the caller.
 */
public interface LocalTmpStorageConfig
{
  /**
   * Get a temporary directory.
   *
   * @return a temporary directory
  */
  File getTmpDir();

  class DefaultLocalTmpStorageConfigProvider implements Provider<LocalTmpStorageConfig>
  {
    private final String prefix;

    public DefaultLocalTmpStorageConfigProvider(String prefix)
    {
      this.prefix = prefix;
    }

    @Override
    public LocalTmpStorageConfig get()
    {
      File result = FileUtils.createTempDir(prefix);
      return () -> result;
    }
  }
}
