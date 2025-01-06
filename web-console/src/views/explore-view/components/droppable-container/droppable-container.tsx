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

import classNames from 'classnames';
import type { Column } from 'druid-query-toolkit';
import type React from 'react';
import { forwardRef, useState } from 'react';

import { DragHelper } from '../../drag-helper';
import type { Measure } from '../../models';

import './droppable-container.scss';

export interface DroppableContainerProps extends React.HTMLAttributes<HTMLDivElement> {
  onDropColumn(column: Column): void;
  onDropMeasure?(measure: Measure): void;
  children?: React.ReactNode;
}

export const DroppableContainer = forwardRef<HTMLDivElement, DroppableContainerProps>(
  function DroppableContainer(props, ref) {
    const { className, onDropColumn, onDropMeasure, children, ...rest } = props;
    const [dropHover, setDropHover] = useState(false);

    return (
      <div
        ref={ref}
        className={classNames('droppable-container', className, { 'drop-hover': dropHover })}
        {...rest}
        onDragOver={e => {
          if (!DragHelper.dragColumn && !(onDropMeasure && DragHelper.dragMeasure)) return;
          e.preventDefault();
          e.dataTransfer.dropEffect = 'move';
          setDropHover(true);
        }}
        onDragLeave={e => {
          const currentTarget = e.currentTarget;
          const relatedTarget = e.relatedTarget;
          if (currentTarget.contains(relatedTarget as any)) return;
          setDropHover(false);
        }}
        onDrop={() => {
          if (!DragHelper.dragColumn && !(onDropMeasure && DragHelper.dragMeasure)) return;
          const dragColumn = DragHelper.dragColumn;
          const dragMeasure = DragHelper.dragMeasure;
          setDropHover(false);
          if (dragColumn) {
            DragHelper.dragColumn = undefined;
            onDropColumn(dragColumn);
          } else if (dragMeasure && onDropMeasure) {
            DragHelper.dragMeasure = undefined;
            onDropMeasure(dragMeasure);
          }
        }}
      >
        {children}
      </div>
    );
  },
);
