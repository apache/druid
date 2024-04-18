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

package org.apache.druid.segment.realtime.appenderator;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.RowSignature;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TaskSegmentSchemaUtil
{
  /**
   * Generates segment schema from the segment file.
   */
  public static SchemaPayloadPlus getSegmentSchema(File segmentFile, IndexIO indexIO) throws IOException
  {
    final QueryableIndex queryableIndex = indexIO.loadIndex(segmentFile);
    final StorageAdapter storageAdapter = new QueryableIndexStorageAdapter(queryableIndex);
    final RowSignature rowSignature = storageAdapter.getRowSignature();
    final long numRows = storageAdapter.getNumRows();
    final AggregatorFactory[] aggregatorFactories = storageAdapter.getMetadata().getAggregators();
    Map<String, AggregatorFactory> aggregatorFactoryMap = new HashMap<>();
    if (null != aggregatorFactories) {
      for (AggregatorFactory aggregatorFactory : aggregatorFactories) {
        aggregatorFactoryMap.put(aggregatorFactory.getName(), aggregatorFactory);
      }
    }
    return new SchemaPayloadPlus(new SchemaPayload(rowSignature, aggregatorFactoryMap), numRows);
  }
}
