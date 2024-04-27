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

import { Classes, Position, Tag } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import type { ExpressionMeta } from '@druid-toolkit/visuals-core';
import classNames from 'classnames';
import React from 'react';

import { AggregateMenu } from './aggregate-menu';

export interface AggregatesInputProps {
  columns: ExpressionMeta[];
  value: ExpressionMeta[];
  onValueChange(value: ExpressionMeta[]): void;
  allowDuplicates?: boolean;
}

export const AggregatesInput = function AggregatesInput(props: AggregatesInputProps) {
  const { columns, value, onValueChange, allowDuplicates } = props;

  const availableColumn = allowDuplicates
    ? columns
    : columns.filter(o => !value.find(_ => _.name === o.name));

  return (
    <div className={classNames('aggregates-input', Classes.INPUT, Classes.TAG_INPUT, Classes.FILL)}>
      <div className={Classes.TAG_INPUT_VALUES}>
        {value.map((c, i) => (
          <Tag
            interactive
            key={i}
            onRemove={() => {
              onValueChange(value.filter(v => v !== c));
            }}
          >
            {c.name}
          </Tag>
        ))}
        <Popover2
          position={Position.BOTTOM}
          content={
            <AggregateMenu
              columns={availableColumn}
              onSelectAggregate={c => onValueChange(value.concat(c))}
            />
          }
        >
          <Tag icon={IconNames.PLUS} interactive />
        </Popover2>
      </div>
    </div>
  );
};
