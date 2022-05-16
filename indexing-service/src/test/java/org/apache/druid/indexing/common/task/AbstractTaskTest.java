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

package org.apache.druid.indexing.common.task;

import org.junit.Assert;
import org.junit.Test;

public class AbstractTaskTest
{

  @Test
  public void testBatchIOConfigAppend()
  {
    AbstractTask.IngestionMode ingestionMode = AbstractTask.IngestionMode.fromString("APPEND");
    Assert.assertEquals(AbstractTask.IngestionMode.APPEND, ingestionMode);
  }

  @Test
  public void testBatchIOConfigReplace()
  {
    AbstractTask.IngestionMode ingestionMode = AbstractTask.IngestionMode.fromString("REPLACE");
    Assert.assertEquals(AbstractTask.IngestionMode.REPLACE, ingestionMode);
  }

  @Test
  public void testBatchIOConfigOverwrite()
  {
    AbstractTask.IngestionMode ingestionMode = AbstractTask.IngestionMode.fromString("REPLACE_LEGACY");
    Assert.assertEquals(AbstractTask.IngestionMode.REPLACE_LEGACY, ingestionMode);
  }

  @Test
  public void testBatchIOConfigHadoop()
  {
    AbstractTask.IngestionMode ingestionMode = AbstractTask.IngestionMode.fromString("HADOOP");
    Assert.assertEquals(AbstractTask.IngestionMode.HADOOP, ingestionMode);
  }

  @Test
  public void testBatchIOConfigNone()
  {
    AbstractTask.IngestionMode ingestionMode = AbstractTask.IngestionMode.fromString("NONE");
    Assert.assertEquals(AbstractTask.IngestionMode.NONE, ingestionMode);
  }

}
