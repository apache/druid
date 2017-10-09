/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.security.basic.db.derby;

import com.google.common.base.Throwables;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.derby.drda.NetworkServerControl;

import java.net.InetAddress;

public class DerbyAuthorizationStorage
{
  private static final Logger log = new Logger(DerbyAuthorizationStorage.class);

  private final NetworkServerControl server;

  public DerbyAuthorizationStorage(MetadataStorageConnectorConfig config)
  {
    try {
      this.server = new NetworkServerControl(InetAddress.getByName(config.getHost()), config.getPort());
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void start()
  {
    try {
      log.info("Starting Derby Authorization Storage");
      server.start(null);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void stop()
  {
    try {
      log.info("Stopping Derby Authorization Storage");
      server.shutdown();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
