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

package org.apache.druid.spark

import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory
import org.apache.druid.segment.{IndexIO, IndexMergerV9}

package object v2 { // scalastyle:ignore package.object.name
  val DruidDataSourceV2ShortName = "druid"

  private[v2] val INDEX_IO = new IndexIO(
    MAPPER,
    () => 1000000
  )

  private[v2] val INDEX_MERGER_V9 = new IndexMergerV9(
    MAPPER,
    INDEX_IO,
    OnHeapMemorySegmentWriteOutMediumFactory.instance()
  )
}
