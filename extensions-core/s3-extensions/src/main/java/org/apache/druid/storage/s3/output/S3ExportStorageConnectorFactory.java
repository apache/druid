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

package org.apache.druid.storage.s3.output;

import com.google.inject.Injector;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.storage.StorageConnectorProvider;
import org.apache.druid.storage.export.ExportStorageConnectorFactory;

import java.io.File;
import java.util.Map;

public class S3ExportStorageConnectorFactory implements ExportStorageConnectorFactory
{
  @Override
  public StorageConnectorProvider get(Map<String, String> properties, Injector injector)
  {
    String chunkSize = properties.getOrDefault(S3StorageConnectorProvider.CHUNK_SIZE_FIELD_NAME, null);
    Integer maxRetries = QueryContexts.getAsInt(
        S3StorageConnectorProvider.MAX_RETRY_FIELD_NAME,
        properties.getOrDefault(S3StorageConnectorProvider.MAX_RETRY_FIELD_NAME, null));

    return new S3StorageConnectorProvider(
        properties.get(S3StorageConnectorProvider.BUCKET_FIELD_NAME),
        properties.get(S3StorageConnectorProvider.PREFIX_FIELD_NAME),
        new File(properties.get(S3StorageConnectorProvider.TEMP_DIR_FIELD_NAME)),
        QueryContexts.getAsHumanReadableBytes(S3StorageConnectorProvider.CHUNK_SIZE_FIELD_NAME, chunkSize, null),
        maxRetries,
        injector
        );
  }
}
