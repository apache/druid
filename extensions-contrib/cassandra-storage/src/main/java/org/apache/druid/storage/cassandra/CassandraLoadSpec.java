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

package org.apache.druid.storage.cassandra;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.segment.loading.SegmentLoadingException;

import java.io.File;

/**
 *
 */
@JsonTypeName(CassandraDruidModule.SCHEME)
public class CassandraLoadSpec implements LoadSpec
{
  @JsonProperty
  private final String key;
  private final CassandraDataSegmentPuller puller;

  @JsonCreator
  public CassandraLoadSpec(
      @JacksonInject CassandraDataSegmentPuller puller,
      @JsonProperty("key") String key
  )
  {
    this.puller = puller;
    this.key = key;
  }

  @Override
  public LoadSpecResult loadSegment(File outDir) throws SegmentLoadingException
  {
    return new LoadSpecResult(puller.getSegmentFiles(key, outDir).size());
  }
}
