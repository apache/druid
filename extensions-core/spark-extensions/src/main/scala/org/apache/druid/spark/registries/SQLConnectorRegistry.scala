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
import org.apache.druid.metadata.storage.mysql.{MySQLConnector, MySQLConnectorDriverConfig,
  MySQLConnectorSslConfig}
import org.apache.druid.metadata.storage.postgresql.{PostgreSQLConnector,
  PostgreSQLConnectorConfig, PostgreSQLTablesConfig}
import org.apache.druid.metadata.{MetadataStorageConnectorConfig, MetadataStorageTablesConfig,
  SQLMetadataConnector}
import org.apache.druid.spark.mixins.Logging

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

  /**
    * Register the provided creation function for the metadata server type SQLCONNECTORTYPE. This function should take
    * two arguments, a `Supplier[MetadataStorageConnectorConfig]` and a `Supplier[MetadataStorageTablesConfig]`. These
    * configs are parsed from the `metadata.*` properties specified when calling read() or .write() on a dataframe.
    *
    * @param sqlConnectorType The SQL database type to create a connector for.
    * @param createFunc The function to use to create a SQLMetadataConnector for SQLCONNECTORTYPE.
    */
  def register(sqlConnectorType: String,
               createFunc:
               (Supplier[MetadataStorageConnectorConfig], Supplier[MetadataStorageTablesConfig]) =>
                 SQLMetadataConnector): Unit = {
    registeredSQLConnectorFunctions(sqlConnectorType) = createFunc
  }

  /**
    * Register the known SQLMetadataConnector for the given SQLCONNECTORTYPE. Known SQL Connector types and the
    * corresponding creator functions are defined in `SQLConnectorRegistry.knownTypes`.
    *
    * @param sqlConnectorType The known SQL Connector type to register a bundled creation function for.
    */
  def registerByType(sqlConnectorType: String): Unit = {
    if (!registeredSQLConnectorFunctions.contains(sqlConnectorType)
      && knownTypes.contains(sqlConnectorType)) {
      register(sqlConnectorType, knownTypes(sqlConnectorType))
    }
  }

  /**
    * Return a SQLMetadataConnector using the creation function registered for SQLCONNECTORTYPE. SQLCONNECTORTYPE must
    * have either already been registered via `register(sqlConnectorType, ...)` or must be a known type.
    *
    * @param sqlConnectorType The SQL database type to create a Connector for.
    * @param connectorConfigSupplier The supplier for the connector config used to configure the returned connector.
    * @param metadataTableConfigSupplier The supplier for the metadata table config used to configure the returned
    *                                    connector.
    * @return A SQLMetadataConnector capable of querying an instance of a metadata server database of type
    *         SQLCONNECTORTYPE, configured according to the provided config suppliers.
    */
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
            new MySQLConnectorSslConfig,
            new MySQLConnectorDriverConfig)
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
