/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { L } from 'druid-query-toolkit';
import React from 'react';
import ReactTable from 'react-table';

import { Loader } from '../../../components';
import { useQueryManager } from '../../../hooks';
import { STANDARD_TABLE_PAGE_SIZE, STANDARD_TABLE_PAGE_SIZE_OPTIONS } from '../../../react-table';
import { queryDruidSql } from '../../../utils';

import './service-properties-table.scss';

export interface ServicePropertiesTableRow {
  property: string;
  value: string;
  service_name: string;
  node_roles: string | null;
}

export interface ServicePropertiesTableProps {
  server: string;
}

export const ServicePropertiesTable = React.memo(function ServicePropertiesTable(
  props: ServicePropertiesTableProps,
) {
  const [propertiesState] = useQueryManager<string, ServicePropertiesTableRow[]>({
    initQuery: props.server,
    processQuery: async (server, signal) => {
      return await queryDruidSql<ServicePropertiesTableRow>(
        {
          query: `SELECT "property", "value", "service_name", "node_roles" FROM sys.server_properties WHERE "server" = ${L(
            server,
          )} ORDER BY "property" ASC`,
        },
        signal,
      );
    },
  });

  function renderTable() {
    const properties = propertiesState.data || [];
    const firstRow = properties[0];
    return (
      <>
        {firstRow && (
          <p
            className="service-info"
            data-tooltip={firstRow.node_roles ? `Operating as ${firstRow.node_roles}` : undefined}
          >
            Server <strong>{props.server}</strong> is running as{' '}
            <strong>{firstRow.service_name}</strong>.
          </p>
        )}
        <ReactTable
          data={properties}
          defaultPageSize={STANDARD_TABLE_PAGE_SIZE}
          pageSizeOptions={STANDARD_TABLE_PAGE_SIZE_OPTIONS}
          showPagination={properties.length > STANDARD_TABLE_PAGE_SIZE}
          filterable
          columns={[
            {
              Header: 'Property',
              accessor: 'property',
              width: 300,
              className: 'padded',
            },
            {
              Header: 'Value',
              accessor: 'value',
              width: 600,
              className: 'padded code',
            },
          ]}
          noDataText={propertiesState.getErrorMessage() || 'No properties found'}
        />
      </>
    );
  }

  return (
    <div className="service-properties-table">
      {propertiesState.loading ? <Loader /> : renderTable()}
    </div>
  );
});
