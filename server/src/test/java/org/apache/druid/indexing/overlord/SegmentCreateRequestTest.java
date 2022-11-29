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

package org.apache.druid.indexing.overlord;

import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.apache.druid.timeline.partition.PartialShardSpec;
import org.junit.Assert;
import org.junit.Test;

public class SegmentCreateRequestTest
{

  @Test
  public void testNullPreviousSegmentId()
  {
    final PartialShardSpec partialShardSpec = NumberedPartialShardSpec.instance();
    SegmentCreateRequest request = new SegmentCreateRequest(
        "sequence",
        null,
        "version",
        partialShardSpec
    );
    Assert.assertEquals("sequence", request.getSequenceName());
    Assert.assertEquals("", request.getPreviousSegmentId());
    Assert.assertEquals("version", request.getVersion());
    Assert.assertEquals(partialShardSpec, request.getPartialShardSpec());
  }

}
