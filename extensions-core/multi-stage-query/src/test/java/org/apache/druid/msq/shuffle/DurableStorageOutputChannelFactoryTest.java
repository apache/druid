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

package org.apache.druid.msq.shuffle;

import org.apache.druid.frame.processor.OutputChannelFactoryTest;
import org.apache.druid.storage.local.LocalFileStorageConnector;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class DurableStorageOutputChannelFactoryTest extends OutputChannelFactoryTest
{
  @ClassRule
  public static TemporaryFolder folder = new TemporaryFolder();

  public DurableStorageOutputChannelFactoryTest()
      throws IOException
  {
    super(
        new DurableStorageOutputChannelFactory(
            "0",
            0,
            0,
            "0",
            100,
            new LocalFileStorageConnector(folder.newFolder()),
            folder.newFolder()
        ),
        100
    );
  }
}
