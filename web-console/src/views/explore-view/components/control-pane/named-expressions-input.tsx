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

import { Classes, Popover, Position, Tag } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import type { JSX } from 'react';
import type React from 'react';
import { useCallback, useState } from 'react';

import type { ExpressionMeta, Measure } from '../../models';

import './named-expressions-input.scss';

function moveInArray(arr: any[], fromIndex: number, toIndex: number) {
  arr = arr.concat();
  const element = arr[fromIndex];
  arr.splice(fromIndex, 1);
  arr.splice(toIndex, 0, element);

  return arr;
}

export interface NamesExpressionsInputProps<M extends ExpressionMeta | Measure> {
  values: M[];
  onValuesChange(value: M[]): void;
  allowReordering?: boolean;
  singleton?: boolean;
  nonEmpty?: boolean;
  itemMenu: (item: M | undefined, onClose: () => void) => JSX.Element;
}

export const NamedExpressionsInput = function NamedExpressionsInput<
  M extends ExpressionMeta | Measure,
>(props: NamesExpressionsInputProps<M>) {
  const { values, onValuesChange, allowReordering, singleton, nonEmpty, itemMenu } = props;

  const [dragIndex, setDragIndex] = useState(-1);
  const [dropBefore, setDropBefore] = useState(false);
  const [dropIndex, setDropIndex] = useState(-1);
  const [menuOpenOn, setMenuOpenOn] = useState<{ openOn?: M }>();

  const startDrag = useCallback((e: React.DragEvent, i: number) => {
    e.dataTransfer.effectAllowed = 'move';
    setDragIndex(i);
  }, []);

  const onDragOver = useCallback(
    (e: React.DragEvent, i: number) => {
      if (dragIndex === -1) return;
      const targetRect = e.currentTarget.getBoundingClientRect();
      const before = e.clientX - targetRect.left <= targetRect.width / 2;
      setDropBefore(before);

      e.preventDefault();

      if (i === dropIndex) return;

      setDropIndex(i);
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
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
          onValuesChange(moveInArray(values, dragIndex, correctedDropIndex));
        }
      }
      setDragIndex(-1);
      setDropIndex(-1);
      setDropBefore(false);
    },
    [dropIndex, dragIndex, onValuesChange, values, dropBefore],
  );

  const menuOnClose = () => {
    setMenuOpenOn(undefined);
  };

  const canRemove = !nonEmpty || values.length > 1;
  return (
    <div
      className={classNames(
        'named-expressions-input',
        Classes.INPUT,
        Classes.TAG_INPUT,
        Classes.FILL,
      )}
    >
      <div className={Classes.TAG_INPUT_VALUES} onDragEnd={onDrop}>
        {values.map((c, i) => (
          <Popover
            key={i}
            isOpen={Boolean(menuOpenOn && menuOpenOn.openOn === c)}
            position={Position.BOTTOM}
            onClose={menuOnClose}
            content={itemMenu(c, menuOnClose)}
          >
            <Tag
              className={classNames({
                'drop-before': dropIndex === i && dropBefore,
                'drop-after': dropIndex === i && !dropBefore,
              })}
              title={`Expression: ${c.expression}`}
              interactive
              onClick={() => setMenuOpenOn({ openOn: c })}
              draggable={allowReordering}
              onDragOver={e => onDragOver(e, i)}
              onDragStart={e => startDrag(e, i)}
              onRemove={
                canRemove
                  ? () => {
                      onValuesChange(values.filter(v => v !== c));
                    }
                  : undefined
              }
            >
              {c.name}
            </Tag>
          </Popover>
        ))}
        {(!singleton || !values.length) && (
          <Popover
            isOpen={Boolean(menuOpenOn && !menuOpenOn.openOn)}
            position={Position.BOTTOM}
            onClose={menuOnClose}
            content={itemMenu(undefined, menuOnClose)}
          >
            <Tag icon={IconNames.PLUS} interactive onClick={() => setMenuOpenOn({})} />
          </Popover>
        )}
      </div>
    </div>
  );
};
