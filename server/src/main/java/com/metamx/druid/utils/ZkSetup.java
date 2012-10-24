/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.utils;

import com.metamx.druid.db.DbConnector;
import com.metamx.druid.db.DbConnectorConfig;
import com.metamx.druid.zk.StringZkSerializer;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

/**
 */
public class ZkSetup
{
  public static void main(final String[] args)
  {
    if (args.length != 5) {
      System.out.println("Usage: <java invocation> zkConnect baseZkPath dbConnectionUrl dbUsername:password tableName");
      System.exit(1);
    }

    String path = args[1];

    String[] subPaths = new String[]{"announcements", "servedSegments", "loadQueue", "master"};

    final ZkClient zkClient = new ZkClient(
        new ZkConnection(args[0]),
        Integer.MAX_VALUE,
        new StringZkSerializer()
    );

    zkClient.createPersistent(path, true);
    for (String subPath : subPaths) {
      final String thePath = String.format("%s/%s", path, subPath);
      if (zkClient.exists(thePath)) {
        System.out.printf("Path[%s] exists already%n", thePath);
      }
      else {
        System.out.printf("Creating ZK path[%s]%n", thePath);
        zkClient.createPersistent(thePath);
      }
    }

    final DbConnectorConfig config = new DbConnectorConfig()
    {
      private final String username;
      private final String password;

      {
        username = args[3].split(":")[0];
        password = args[3].split(":")[1];
      }

      @Override
      public String getDatabaseConnectURI()
      {
        return args[2];
      }

      @Override
      public String getDatabaseUser()
      {
        return username;
      }

      @Override
      public String getDatabasePassword()
      {
        return password;
      }

      @Override
      public String getSegmentTable()
      {
        return args[4];
      }
    };

    DbConnector dbConnector = new DbConnector(config);

    DbConnector.createSegmentTable(dbConnector.getDBI(), config);
  }
}
