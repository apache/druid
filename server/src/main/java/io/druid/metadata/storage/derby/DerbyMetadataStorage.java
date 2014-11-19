/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.metadata.storage.derby;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.guice.ManageLifecycle;
import io.druid.metadata.MetadataStorage;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.derby.drda.NetworkServerControl;

import java.net.InetAddress;


@ManageLifecycle
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
      throw Throwables.propagate(e);
    }

  }

  @Override
  @LifecycleStart
  public void start()
  {
    try {
      log.info("Starting Derby Metadata Storage");
      server.start(null);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    try {
      server.shutdown();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
