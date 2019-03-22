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

package org.apache.druid.metadata.storage.derby;

import com.google.inject.Inject;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.MetadataStorage;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;

import java.net.InetAddress;

public class DerbyMetadataStorage extends MetadataStorage
{
  private static final Logger log = new Logger(DerbyMetadataStorage.class);

  private final NetworkServerControl server;

  @Inject
  public DerbyMetadataStorage(MetadataStorageConnectorConfig config)
  {
    try {
      this.server = new NetworkServerControl(InetAddress.getByName(config.getHost()), config.getPort());
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void start()
  {
    try {
      log.info("Starting Derby Metadata Storage");
      server.start(null);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop()
  {
    try {
      server.shutdown();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
