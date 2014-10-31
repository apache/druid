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
package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.lang.Exception;
import java.util.LinkedList;


public class SQLMetadataConnectorTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private DerbyConnector connector;
  private DBI dbi;
  private MetadataStorageTablesConfig tablesConfig = MetadataStorageTablesConfig.fromBase("test");
  private String TABLE_NAME = tablesConfig.getSegmentsTable();

  @Before
  public void setUp() {
    MetadataStorageConnectorConfig config = jsonReadWriteRead(
        "{"
        + "\"type\" : \"db\",\n"
        + "\"segmentTable\" : \"segments\"\n"
        + "}",
        MetadataStorageConnectorConfig.class
    );

    connector = new DerbyConnector(
        Suppliers.ofInstance(config),
        Suppliers.ofInstance(tablesConfig)
    );
    dbi = connector.getDBI();
  }

  @Test
  public void testCreateTables()
  {
    final LinkedList<String> tables = new LinkedList<String>();
    tables.add(tablesConfig.getConfigTable());
    tables.add(tablesConfig.getSegmentsTable());
    tables.add(tablesConfig.getRulesTable());
    tables.add(tablesConfig.getTaskLockTable());
    tables.add(tablesConfig.getTaskLogTable());
    tables.add(tablesConfig.getTasksTable());
    try {
      connector.createSegmentTable();
      connector.createConfigTable();
      connector.createRulesTable();
      connector.createTaskTables();

      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              for(String table : tables) {
                Assert.assertTrue(
                    String.format("table $s was not created!", table),
                    connector.tableExists(handle, table)
                );
              }

              return null;
            }
          }
      );
    }
    finally {
      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              final Batch batch = handle.createBatch();
              for (String table : tables) {
                batch.add(String.format("DROP TABLE %s", table));
              }
              batch.execute();
              return null;
            }
          }
      );
    }
  }

  @Test
  public void testInsertOrUpdate() {
    try {
      connector.createSegmentTable(dbi, TABLE_NAME);
      connector.insertOrUpdate(TABLE_NAME, "dummy1", "dummy2", "emperor", "penguin".getBytes());
      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              Assert.assertEquals(connector.lookup(TABLE_NAME, "dummy1", "dummy2", "emperor"),
                                  "penguin".getBytes());
              return null;
            }
          }
      );

      connector.insertOrUpdate(TABLE_NAME, "dummy1", "dummy2", "emperor", "penguin chick".getBytes());

      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              Assert.assertEquals(connector.lookup(TABLE_NAME, "dummy1", "dummy2", "emperor"),
                                  "penguin chick".getBytes());
              return null;
            }
          }
      );

    } catch (Exception e) {
    } finally {
      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              handle.createStatement(String.format("DROP TABLE %s", TABLE_NAME))
                    .execute();
              return null;
            }
          }
      );
    }
  }

  private <T> T jsonReadWriteRead(String s, Class<T> klass)
  {
    try {
      return jsonMapper.readValue(jsonMapper.writeValueAsBytes(jsonMapper.readValue(s, klass)), klass);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
