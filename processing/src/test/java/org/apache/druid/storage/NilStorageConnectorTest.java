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

package org.apache.druid.storage;

import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NilStorageConnectorTest
{

  private static String ERROR_MESSAGE = "Please configure durable storage.";


  @Test
  public void sanity()
  {
    NilStorageConnector nilStorageConnector = NilStorageConnector.getInstance();
    Assertions.assertThrows(DruidException.class, () -> nilStorageConnector.pathExists("null"));
    Assertions.assertThrows(DruidException.class, () -> nilStorageConnector.read("null"));
    Assertions.assertThrows(DruidException.class, () -> nilStorageConnector.readRange("null", 0, 0));
    Assertions.assertThrows(DruidException.class, () -> nilStorageConnector.deleteFile("null"));
    Assertions.assertThrows(DruidException.class, () -> nilStorageConnector.deleteFiles(ImmutableList.of()));
    Assertions.assertThrows(DruidException.class, () -> nilStorageConnector.deleteRecursively("null"));
    Assertions.assertThrows(DruidException.class, () -> nilStorageConnector.listDir("null"));
    Assertions.assertThrows(DruidException.class, () -> nilStorageConnector.pathExists("null"));
  }

}
