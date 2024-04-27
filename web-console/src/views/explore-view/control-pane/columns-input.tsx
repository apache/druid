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
import type { JSX } from 'react';
import React, { useCallback, useState } from 'react';

import { ColumnPickerMenu } from '../column-picker-menu/column-picker-menu';

import './columns-input.scss';

export interface ColumnsInputProps {
  columns: ExpressionMeta[];
  value: ExpressionMeta[];
  onValueChange(value: ExpressionMeta[]): void;
  allowDuplicates?: boolean;
  allowReordering?: boolean;

  /**
   * If you want to take control of the way new columns are picked and added
   */
  pickerMenu?: (columns: ExpressionMeta[]) => JSX.Element;
}

function moveInArray(arr: any[], fromIndex: number, toIndex: number) {
  arr = arr.concat();
  const element = arr[fromIndex];
  arr.splice(fromIndex, 1);
  arr.splice(toIndex, 0, element);

  return arr;
}

export const ColumnsInput = function ColumnsInput(props: ColumnsInputProps) {
  const { columns, value, onValueChange, allowDuplicates, allowReordering, pickerMenu } = props;

  const availableColumns = allowDuplicates
    ? columns
    : columns.filter(o => !value.find(_ => _.name === o.name));

  const [dragIndex, setDragIndex] = useState(-1);
  const [dropBefore, setDropBefore] = useState(false);
  const [dropIndex, setDropIndex] = useState(-1);

  const startDrag = useCallback((e: React.DragEvent, i: number) => {
    e.dataTransfer.effectAllowed = 'move';
    setDragIndex(i);
  }, []);

  const onDragOver = useCallback(
    (e: React.DragEvent, i: number) => {
      const targetRect = e.currentTarget.getBoundingClientRect();
      const before = e.clientX - targetRect.left <= targetRect.width / 2;
      setDropBefore(before);

      e.preventDefault();

      if (i === dropIndex) return;

      setDropIndex(i);
    },
    [dropIndex],
  );

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      if (dropIndex > -1) {
        let correctedDropIndex = dropIndex + (dropBefore ? 0 : 1);
        if (correctedDropIndex > dragIndex) correctedDropIndex--;

        if (correctedDropIndex !== dragIndex) {
          onValueChange(moveInArray(value, dragIndex, correctedDropIndex));
        }
      }
      setDragIndex(-1);
      setDropIndex(-1);
      setDropBefore(false);
    },
    [dropIndex, dragIndex, onValueChange, value, dropBefore],
  );

  return (
    <div className={classNames('columns-input', Classes.INPUT, Classes.TAG_INPUT, Classes.FILL)}>
      <div className={Classes.TAG_INPUT_VALUES} onDragEnd={onDrop}>
        {value.map((c, i) => (
          <Tag
            className={classNames({
              'drop-before': dropIndex === i && dropBefore,
              'drop-after': dropIndex === i && !dropBefore,
            })}
            interactive
            draggable={allowReordering}
            onDragOver={e => onDragOver(e, i)}
            key={i}
            onDragStart={e => startDrag(e, i)}
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
            pickerMenu ? (
              pickerMenu(availableColumns)
            ) : (
              <ColumnPickerMenu
                columns={availableColumns}
                onSelectColumn={c => onValueChange(value.concat(c))}
              />
            )
          }
        >
          <Tag icon={IconNames.PLUS} interactive />
        </Popover2>
      </div>
    </div>
  );
};
