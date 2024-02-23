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
import org.junit.Assert;
import org.junit.Test;

public class NilStorageConnectorTest
{

  private static String ERROR_MESSAGE = "Please configure durable storage.";


  @Test
  public void sanity()
  {
    NilStorageConnector nilStorageConnector = NilStorageConnector.getInstance();
    Assert.assertThrows(ERROR_MESSAGE, DruidException.class, () -> nilStorageConnector.pathExists("null"));
    Assert.assertThrows(ERROR_MESSAGE, DruidException.class, () -> nilStorageConnector.read("null"));
    Assert.assertThrows(ERROR_MESSAGE, DruidException.class, () -> nilStorageConnector.readRange("null", 0, 0));
    Assert.assertThrows(ERROR_MESSAGE, DruidException.class, () -> nilStorageConnector.deleteFile("null"));
    Assert.assertThrows(ERROR_MESSAGE, DruidException.class, () -> nilStorageConnector.deleteFiles(ImmutableList.of()));
    Assert.assertThrows(ERROR_MESSAGE, DruidException.class, () -> nilStorageConnector.deleteRecursively("null"));
    Assert.assertThrows(ERROR_MESSAGE, DruidException.class, () -> nilStorageConnector.listDir("null"));
    Assert.assertThrows(ERROR_MESSAGE, DruidException.class, () -> nilStorageConnector.pathExists("null"));
  }

}
