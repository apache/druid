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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorTuningConfig;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.ServiceClientFactory;

public abstract class SeekableStreamIndexTaskClientFactory<PartitionIdType, SequenceOffsetType>
{
  private static final Logger log = new Logger(SeekableStreamIndexTaskClientFactory.class);

  private final ServiceClientFactory serviceClientFactory;
  private final ObjectMapper jsonMapper;

  protected SeekableStreamIndexTaskClientFactory(
      final ServiceClientFactory serviceClientFactory,
      final ObjectMapper jsonMapper
  )
  {
    this.serviceClientFactory = serviceClientFactory;
    this.jsonMapper = jsonMapper;
  }

  public SeekableStreamIndexTaskClient<PartitionIdType, SequenceOffsetType> build(
      final String dataSource,
      final TaskInfoProvider taskInfoProvider,
      final int maxNumTasks,
      final SeekableStreamSupervisorTuningConfig tuningConfig
  )
  {
    log.info(
        "Created async task client for dataSource[%s] httpTimeout[%s] chatRetries[%d]",
        dataSource,
        tuningConfig.getHttpTimeout(),
        tuningConfig.getChatRetries()
    );

    return new SeekableStreamIndexTaskClientAsyncImpl<PartitionIdType, SequenceOffsetType>(
        dataSource,
        serviceClientFactory,
        taskInfoProvider,
        jsonMapper,
        tuningConfig.getHttpTimeout(),
        tuningConfig.getChatRetries()
    )
    {
      @Override
      public Class<PartitionIdType> getPartitionType()
      {
        return SeekableStreamIndexTaskClientFactory.this.getPartitionType();
      }

      @Override
      public Class<SequenceOffsetType> getSequenceType()
      {
        return SeekableStreamIndexTaskClientFactory.this.getSequenceType();
      }
    };
  }

  protected abstract Class<PartitionIdType> getPartitionType();

  protected abstract Class<SequenceOffsetType> getSequenceType();
}
