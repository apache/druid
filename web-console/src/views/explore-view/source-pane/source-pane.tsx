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

import { Button, Menu, MenuDivider, MenuItem, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import React from 'react';

import { useQueryManager } from '../../../hooks';
import { queryDruidSql } from '../../../utils';

import './source-pane.scss';

export interface SourcePaneProps {
  selectedTableName: string;
  onSelectedTableNameChange(newSelectedSource: string): void;
  disabled?: boolean;
}

export const SourcePane = React.memo(function SourcePane(props: SourcePaneProps) {
  const { selectedTableName, onSelectedTableNameChange, disabled } = props;

  const [sources] = useQueryManager<string, string[]>({
    initQuery: '',
    processQuery: async () => {
      const tables = await queryDruidSql<{ TABLE_NAME: string }>({
        query: `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'TABLE'`,
      });

      return tables.map(d => d.TABLE_NAME);
    },
  });

  return (
    <Popover2
      className="source-pane"
      disabled={disabled}
      minimal
      position={Position.BOTTOM_LEFT}
      content={
        <Menu className="source-menu">
          {sources.loading && <MenuDivider title="Loading..." />}
          {sources.data?.map((s, i) => (
            <MenuItem key={i} text={s} onClick={() => onSelectedTableNameChange(s)} />
          ))}
          {!sources.data?.length && <MenuItem text="No sources" disabled />}
        </Menu>
      }
    >
      <Button
        text={`Source: ${selectedTableName}`}
        rightIcon={IconNames.CARET_DOWN}
        fill
        minimal
        disabled={disabled}
      />
    </Popover2>
  );
});
