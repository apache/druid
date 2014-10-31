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
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.LinkedList;


public class SQLMetadataConnectorTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig = MetadataStorageTablesConfig.fromBase("test");

  @Before
  public void setUp() throws Exception {
    MetadataStorageConnectorConfig config = jsonReadWriteRead(
          "{"
        + "\"type\" : \"db\",\n"
        + "\"segmentTable\" : \"segments\"\n"
        + "}",
        MetadataStorageConnectorConfig.class
    );

    connector = new TestDerbyConnector(
        Suppliers.ofInstance(config),
        Suppliers.ofInstance(tablesConfig)
    );
  }

  @Test
  public void testCreateTables() throws Exception
  {
    final LinkedList<String> tables = new LinkedList<String>();
    tables.add(tablesConfig.getConfigTable());
    tables.add(tablesConfig.getSegmentsTable());
    tables.add(tablesConfig.getRulesTable());
    tables.add(tablesConfig.getTaskLockTable());
    tables.add(tablesConfig.getTaskLogTable());
    tables.add(tablesConfig.getTasksTable());

    connector.createSegmentTable();
    connector.createConfigTable();
    connector.createRulesTable();
    connector.createTaskTables();

    connector.getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            for (String table : tables) {
              Assert.assertTrue(
                  String.format("table $s was not created!", table),
                  connector.tableExists(handle, table)
              );
            }

            return null;
          }
        }
    );

    for (String table : tables) {
      dropTable(table);
    }
  }

  @Test
  public void testInsertOrUpdate() throws Exception
  {
    final String tableName = "test";
    connector.createConfigTable(connector.getDBI(), tableName);

    Assert.assertNull(connector.lookup(tableName, "name", "payload", "emperor"));

    connector.insertOrUpdate(tableName, "name", "payload", "emperor", "penguin".getBytes());
    Assert.assertArrayEquals(
        "penguin".getBytes(),
        connector.lookup(tableName, "name", "payload", "emperor")
    );

    connector.insertOrUpdate(tableName, "name", "payload", "emperor", "penguin chick".getBytes());

    Assert.assertArrayEquals(
        "penguin chick".getBytes(),
        connector.lookup(tableName, "name", "payload", "emperor")
    );

    dropTable(tableName);
  }

  private void dropTable(final String tableName)
  {
    connector.getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(String.format("DROP TABLE %s", tableName))
                  .execute();
            return null;
          }
        }
    );
  }

  private <T> T jsonReadWriteRead(String s, Class<T> klass) throws Exception
  {
      return jsonMapper.readValue(jsonMapper.writeValueAsBytes(jsonMapper.readValue(s, klass)), klass);
  }
}
