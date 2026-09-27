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

// Stand-ins for heavy child components, so a snapshot can stop at a component boundary the way
// shallow rendering did. Use them from a jest.mock factory, for example:
//
//   jest.mock('react-table', () => jest.requireActual('../../test-utils/stub-component').reactTableStub);

import React from 'react';

function kebabCase(name: string): string {
  return name.replace(/([a-z0-9])([A-Z])/g, '$1-$2').toLowerCase();
}

function primitiveAttributes(props: Record<string, any>): Record<string, string> {
  const attributes: Record<string, string> = {};
  for (const [key, value] of Object.entries(props)) {
    if (key === 'children') continue;
    if (typeof value === 'string' || typeof value === 'number') {
      attributes[kebabCase(key)] = String(value);
    } else if (typeof value === 'boolean') {
      attributes[kebabCase(key)] = String(value);
    }
  }
  return attributes;
}

/**
 * Makes a component that renders as <stub-name> with its string, number and boolean props as attributes
 * and its children rendered inside.
 */
export function stubComponent(name: string): React.FC<any> {
  const Stub = (props: any) =>
    React.createElement(`stub-${kebabCase(name)}`, primitiveAttributes(props), props.children);
  Stub.displayName = name;
  return Stub;
}

interface StubColumn {
  id?: string;
  accessor?: unknown;
  Header?: unknown;
  show?: boolean;
  width?: number;
  columns?: StubColumn[];
}

function renderColumns(columns: StubColumn[] | undefined): React.ReactNode {
  return (columns || []).map((column, i) =>
    React.createElement(
      'stub-column',
      {
        key: i,
        id: column.id ?? (typeof column.accessor === 'string' ? column.accessor : undefined),
        ...(column.show === false ? { hidden: 'true' } : {}),
        ...(column.width ? { width: String(column.width) } : {}),
      },
      typeof column.Header === 'function' ? null : (column.Header as React.ReactNode),
      renderColumns(column.columns),
    ),
  );
}

function StubReactTable(props: any) {
  return React.createElement(
    'stub-react-table',
    {
      ...primitiveAttributes(props),
      rows: String(Array.isArray(props.data) ? props.data.length : 0),
    },
    renderColumns(props.columns),
  );
}

/** A module to mock 'react-table' with: renders the table props and column headers, but no cells */
export const reactTableStub = {
  __esModule: true,
  default: StubReactTable,
};
