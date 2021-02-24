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

package org.apache.druid.spark.registries

import com.google.common.base.Supplier
import org.apache.druid.java.util.common.IAE
import org.apache.druid.metadata.storage.derby.{DerbyConnector, DerbyMetadataStorage}
import org.apache.druid.metadata.storage.mysql.{MySQLConnector, MySQLConnectorConfig}
import org.apache.druid.metadata.storage.postgresql.{PostgreSQLConnector, PostgreSQLConnectorConfig,
  PostgreSQLTablesConfig}
import org.apache.druid.metadata.{MetadataStorageConnectorConfig, MetadataStorageTablesConfig,
  SQLMetadataConnector}
import org.apache.druid.spark.utils.Logging

import scala.collection.mutable

/**
 * A registry for plugging in support for connectors to Druid metadata servers. Supports mysql, postgres, and derby out
 * of the box.
 */
object SQLConnectorRegistry extends Logging {
  private val registeredSQLConnectorFunctions:mutable.HashMap[String,
    (Supplier[MetadataStorageConnectorConfig], Supplier[MetadataStorageTablesConfig]) =>
      SQLMetadataConnector] =
    new mutable.HashMap()

  def register(sqlConnectorType: String,
               loadFunc:
               (Supplier[MetadataStorageConnectorConfig], Supplier[MetadataStorageTablesConfig]) =>
                 SQLMetadataConnector): Unit = {
    registeredSQLConnectorFunctions(sqlConnectorType) = loadFunc
  }

  def registerByType(sqlConnectorType: String): Unit = {
    if (!registeredSQLConnectorFunctions.contains(sqlConnectorType)
      && knownTypes.contains(sqlConnectorType)) {
      register(sqlConnectorType, knownTypes(sqlConnectorType))
    }
  }

  def create(
              sqlConnectorType: String,
              connectorConfigSupplier: Supplier[MetadataStorageConnectorConfig],
              metadataTableConfigSupplier: Supplier[MetadataStorageTablesConfig]
            ): SQLMetadataConnector = {
    if (!registeredSQLConnectorFunctions.contains(sqlConnectorType)) {
      if (knownTypes.contains(sqlConnectorType)) {
        registerByType(sqlConnectorType)
      } else {
        throw new IAE("Unrecognized metadata DB type %s", sqlConnectorType)
      }
    }
    registeredSQLConnectorFunctions(sqlConnectorType)(
      connectorConfigSupplier, metadataTableConfigSupplier)
  }

  private val knownTypes: Map[String,
    (Supplier[MetadataStorageConnectorConfig], Supplier[MetadataStorageTablesConfig]) =>
      SQLMetadataConnector] =
    Map[String, (Supplier[MetadataStorageConnectorConfig], Supplier[MetadataStorageTablesConfig]) =>
      SQLMetadataConnector](
      "mysql" ->
        ((connectorConfigSupplier: Supplier[MetadataStorageConnectorConfig],
          metadataTableConfigSupplier: Supplier[MetadataStorageTablesConfig]) =>
          new MySQLConnector(
            connectorConfigSupplier,
            metadataTableConfigSupplier,
            new MySQLConnectorConfig)
          ),
      "postgres" -> ((connectorConfigSupplier: Supplier[MetadataStorageConnectorConfig],
                       metadataTableConfigSupplier: Supplier[MetadataStorageTablesConfig]) =>
        new PostgreSQLConnector(
          connectorConfigSupplier,
          metadataTableConfigSupplier,
          new PostgreSQLConnectorConfig,
          new PostgreSQLTablesConfig)
        ),
      "derby" -> ((connectorConfigSupplier: Supplier[MetadataStorageConnectorConfig],
                   metadataTableConfigSupplier: Supplier[MetadataStorageTablesConfig]) =>
        new DerbyConnector(
          new DerbyMetadataStorage(connectorConfigSupplier.get()),
          connectorConfigSupplier,
          metadataTableConfigSupplier)
        )
    )
}
