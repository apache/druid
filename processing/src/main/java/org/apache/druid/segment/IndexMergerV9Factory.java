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

package org.apache.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;

public class IndexMergerV9Factory
{
  private final ObjectMapper mapper;
  private final IndexIO indexIO;
  // defaultSegmentWriteOutMediumFactory can be removed as the factory type to use can be determined
  // when the task starts from the task tuningConfig and system property.
  // This just hasn't been done yet, and we should do it at some point.
  private final SegmentWriteOutMediumFactory defaultSegmentWriteOutMediumFactory;

  @Inject
  public IndexMergerV9Factory(
      ObjectMapper mapper,
      IndexIO indexIO,
      SegmentWriteOutMediumFactory defaultSegmentWriteOutMediumFactory
  )
  {
    this.mapper = mapper;
    this.indexIO = indexIO;
    this.defaultSegmentWriteOutMediumFactory = defaultSegmentWriteOutMediumFactory;
  }

  public IndexMergerV9 create(boolean storeEmptyColumns)
  {
    return new IndexMergerV9(mapper, indexIO, defaultSegmentWriteOutMediumFactory, storeEmptyColumns);
  }
}
