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
  HTMLSelect,
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
import { SqlAlias, SqlExpression } from 'druid-query-toolkit';
import { useState } from 'react';

import { AppToaster } from '../../../../singletons';
import { columnToIcon } from '../../../../utils';
import { Measure, MeasurePattern } from '../../models';
import { ColumnPicker } from '../column-picker/column-picker';
import { SqlInput } from '../sql-input/sql-input';

import './measure-menu.scss';

type MeasureMenuTab = 'saved' | 'compose' | 'sql';

export interface MeasureMenuProps {
  columns: readonly Column[];
  measures: readonly Measure[];
  initMeasure: Measure | undefined;
  disabledMeasureNames?: string[];
  onSelectMeasure(measure: Measure): void;
  onClose(): void;
  onAddToSourceQueryAsMeasure?(measure: Measure): void;
}

export const MeasureMenu = function MeasureMenu(props: MeasureMenuProps) {
  const {
    columns,
    measures,
    initMeasure,
    disabledMeasureNames = [],
    onSelectMeasure,
    onClose,
    onAddToSourceQueryAsMeasure,
  } = props;

  const [tab, setTab] = useState<MeasureMenuTab>(() => {
    if (!initMeasure) return measures.length > 1 ? 'saved' : 'compose';
    if (measures.some(measure => measure.equivalent(initMeasure))) return 'saved';
    return MeasurePattern.fit(initMeasure.expression) ? 'compose' : 'sql';
  });
  const [outputName, setOutputName] = useState<string>(initMeasure?.as || '');
  const [measurePattern, setMeasurePattern] = useState<MeasurePattern | undefined>(
    initMeasure ? MeasurePattern.fit(initMeasure.expression) : undefined,
  );
  const [formula, setFormula] = useState<string>(initMeasure ? String(initMeasure.expression) : '');

  function getMeasure(): Measure | undefined {
    switch (tab) {
      case 'saved':
        return;

      case 'compose': {
        if (!measurePattern) throw new Error('no measure pattern');
        const expression = measurePattern.toExpression();
        return new Measure({
          expression,
          as: outputName,
        });
      }

      case 'sql': {
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

        if (parsedFormula instanceof SqlAlias) {
          return new Measure({
            expression: parsedFormula.getUnderlyingExpression(),
            as: outputName || parsedFormula.getAliasName(),
          });
        }

        return new Measure({
          expression: parsedFormula,
          as: outputName,
        });
      }
    }
  }

  const actionDisabled =
    (tab === 'compose' && !measurePattern) || (tab === 'sql' && formula === '');
  return (
    <div className="measure-menu">
      <FormGroup>
        <ButtonGroup className="tab-bar" fill>
          <Button
            text="Saved"
            active={tab === 'saved'}
            onClick={() => {
              setTab('saved');
            }}
          />
          <Button
            text="Compose"
            active={tab === 'compose'}
            onClick={() => {
              if (tab === 'sql') {
                const parsedFormula = SqlExpression.maybeParse(formula);
                if (parsedFormula) {
                  const pattern = MeasurePattern.fit(parsedFormula);
                  if (pattern) {
                    setMeasurePattern(pattern);
                  }
                }
              }
              setTab('compose');
            }}
          />
          <Button
            text="SQL"
            active={tab === 'sql'}
            onClick={() => {
              if (tab === 'compose' && measurePattern) {
                setFormula(String(measurePattern));
              }
              setTab('sql');
            }}
          />
        </ButtonGroup>
      </FormGroup>
      {tab === 'saved' && (
        <Menu>
          {measures.map((measure, i) => {
            const aggregateMeasure = measure.toAggregateBasedMeasure();
            return (
              <MenuItem
                key={i}
                icon={IconNames.PULSE}
                text={aggregateMeasure.name}
                disabled={disabledMeasureNames.includes(measure.name)}
                labelElement={
                  aggregateMeasure.equals(initMeasure) ? <Icon icon={IconNames.TICK} /> : undefined
                }
                onClick={() => {
                  onSelectMeasure(aggregateMeasure);
                }}
              />
            );
          })}
        </Menu>
      )}
      {tab === 'compose' &&
        (measurePattern ? (
          <div className="measure-column-line">
            <FormGroup label="Aggregate">
              <HTMLSelect
                value={measurePattern.aggregate}
                onChange={(e: any) =>
                  setMeasurePattern(measurePattern.changeAggregate(e.target.value))
                }
              >
                {MeasurePattern.AGGREGATES.map(measure => (
                  <option key={measure} value={measure}>
                    {measure}
                  </option>
                ))}
              </HTMLSelect>
            </FormGroup>
            <FormGroup label="Column" className="column-group">
              <ColumnPicker
                availableColumns={columns}
                disabled={measurePattern.aggregate === 'count'}
                selectedColumnName={measurePattern.column}
                onSelectedColumnNameChange={column => {
                  setMeasurePattern(measurePattern.changeColumn(column));
                }}
                fill
                shouldDismissPopover={false}
              />
            </FormGroup>
          </div>
        ) : (
          <Menu>
            {columns.map((c, i) => (
              <MenuItem
                key={i}
                icon={columnToIcon(c)}
                text={c.name}
                shouldDismissPopover={false}
                onClick={() => {
                  setMeasurePattern(MeasurePattern.fromColumn(c));
                }}
              />
            ))}
          </Menu>
        ))}
      {tab === 'sql' && (
        <div className="editor-container">
          <SqlInput
            value={formula}
            onValueChange={setFormula}
            columns={columns}
            placeholder="SQL measure expression"
            autoFocus
          />
        </div>
      )}
      {((tab === 'compose' && measurePattern) || tab === 'sql') && (
        <FormGroup label="Name">
          <InputGroup
            value={outputName}
            placeholder="(default)"
            onChange={e => {
              setOutputName(e.target.value.slice(0, Measure.MAX_NAME_LENGTH));
            }}
          />
        </FormGroup>
      )}
      {tab === 'compose' && measurePattern && <div className="expander" />}
      {tab !== 'saved' && (
        <div className="button-bar">
          {onAddToSourceQueryAsMeasure && (
            <Popover
              disabled={actionDisabled}
              position={Position.BOTTOM_LEFT}
              content={
                <Menu>
                  <MenuItem
                    text="Add as measure to the source query"
                    onClick={() => {
                      const measure = getMeasure();
                      if (!measure) return;

                      onAddToSourceQueryAsMeasure(measure);
                      onClose();
                    }}
                  />
                </Menu>
              }
            >
              <Button icon={IconNames.TH_DERIVED} minimal disabled={actionDisabled} />
            </Popover>
          )}
          <div className="button-separator" />
          <Button text="Cancel" onClick={onClose} />
          <Button
            intent={Intent.PRIMARY}
            text="Apply"
            disabled={actionDisabled}
            onClick={() => {
              const measure = getMeasure();
              if (!measure) return;

              onSelectMeasure(measure);
              onClose();
            }}
          />
        </div>
      )}
    </div>
  );
};
