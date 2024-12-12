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

import {
  Button,
  ButtonGroup,
  FormGroup,
  Icon,
  InputGroup,
  Intent,
  Menu,
  MenuItem,
  Popover,
  Position,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { Column } from 'druid-query-toolkit';
import { SqlColumn, SqlExpression } from 'druid-query-toolkit';
import { useState } from 'react';

import { AppToaster } from '../../../../singletons';
import { columnToIcon } from '../../../../utils';
import { ExpressionMeta } from '../../models';
import { SqlInput } from '../sql-input/sql-input';

import './expression-menu.scss';

type ExpressionMenuTab = 'select' | 'sql';

export interface ExpressionMenuProps {
  columns: readonly Column[];
  initExpression: ExpressionMeta | undefined;
  onSelectExpression(measure: ExpressionMeta): void;
  disabledColumnNames?: readonly string[];
  onClose(): void;
  onAddToSourceQueryAsColumn?(expression: SqlExpression): void;
}

function getColumnIndex(columns: readonly Column[], expression: SqlExpression): number {
  if (!(expression instanceof SqlColumn)) return -1;
  const columnName = expression.getName();
  return columns.findIndex(column => column.name === columnName);
}

export const ExpressionMenu = function ExpressionMenu(props: ExpressionMenuProps) {
  const {
    columns,
    initExpression,
    onSelectExpression,
    disabledColumnNames = [],
    onClose,
    onAddToSourceQueryAsColumn,
  } = props;
  const [tab, setTab] = useState<ExpressionMenuTab>(
    initExpression && getColumnIndex(columns, initExpression.expression) === -1 ? 'sql' : 'select',
  );
  const [outputName, setOutputName] = useState<string>(initExpression?.as || '');
  const [selectedColumnIndex, setSelectedColumnIndex] = useState<number>(
    initExpression ? getColumnIndex(columns, initExpression.expression) : -1,
  );
  const [formula, setFormula] = useState<string>(
    initExpression ? String(initExpression.expression) : '',
  );

  function getExpression(): SqlExpression | undefined {
    if (!formula) {
      AppToaster.show({
        message: 'Formula is empty',
        intent: Intent.DANGER,
      });
      return;
    }

    let parsedFormula: SqlExpression;
    try {
      parsedFormula = SqlExpression.parse(formula);
    } catch (e) {
      AppToaster.show({
        message: `Could not parse formula: ${e.message}`,
        intent: Intent.DANGER,
      });
      return;
    }

    return parsedFormula;
  }

  return (
    <div className="expression-menu">
      <FormGroup>
        <ButtonGroup className="tab-bar" fill>
          <Button
            text="Select"
            active={tab === 'select'}
            onClick={() => {
              if (tab === 'sql') {
                const parsedFormula = SqlExpression.maybeParse(formula);
                if (parsedFormula) {
                  const selectedColumnIndex = getColumnIndex(columns, parsedFormula);
                  if (selectedColumnIndex >= 0) {
                    setSelectedColumnIndex(selectedColumnIndex);
                  }
                }
              }
              setTab('select');
            }}
          />
          <Button
            text="SQL"
            active={tab === 'sql'}
            onClick={() => {
              setTab('sql');
            }}
          />
        </ButtonGroup>
      </FormGroup>
      {tab === 'select' && (
        <Menu>
          {columns.map((c, i) => (
            <MenuItem
              key={i}
              icon={columnToIcon(c)}
              text={c.name}
              disabled={i !== selectedColumnIndex && disabledColumnNames.includes(c.name)}
              labelElement={i === selectedColumnIndex ? <Icon icon={IconNames.TICK} /> : undefined}
              onClick={() => {
                onSelectExpression(ExpressionMeta.fromColumn(c));
              }}
            />
          ))}
        </Menu>
      )}
      {tab === 'sql' && (
        <>
          <div className="editor-container">
            <SqlInput
              value={formula}
              onValueChange={setFormula}
              columns={columns}
              placeholder="SQL expression"
              autoFocus
            />
          </div>
          <FormGroup label="Name">
            <InputGroup
              value={outputName}
              onChange={e => {
                setOutputName(e.target.value.slice(0, ExpressionMeta.MAX_NAME_LENGTH));
              }}
              placeholder="(default)"
            />
          </FormGroup>
          <div className="button-bar">
            {onAddToSourceQueryAsColumn && (
              <Popover
                position={Position.BOTTOM_LEFT}
                content={
                  <Menu>
                    <MenuItem
                      text="Add as column to the source query"
                      onClick={() => {
                        const expression = getExpression();
                        if (!expression) return;
                        onAddToSourceQueryAsColumn(expression.as(outputName || formula));
                        onClose();
                      }}
                    />
                  </Menu>
                }
              >
                <Button icon={IconNames.TH_DERIVED} minimal />
              </Popover>
            )}
            <div className="button-separator" />
            <Button text="Cancel" onClick={onClose} />
            <Button
              intent={Intent.PRIMARY}
              text="Apply"
              onClick={() => {
                const expression = getExpression();
                if (!expression) return;

                onSelectExpression(
                  new ExpressionMeta({
                    expression,
                    as: outputName,
                  }),
                );
                onClose();
              }}
            />
          </div>
        </>
      )}
    </div>
  );
};
