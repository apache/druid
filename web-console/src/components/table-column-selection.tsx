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

import { Button, Checkbox, Popover, Position } from "@blueprintjs/core";
import * as React from 'react';

import { FormGroup, IconNames } from "./filler";

import "./table-column-selection.scss";

interface TableColumnSelectionProps extends React.Props<any> {
  columns: string[];
  onChange: (column: string) => void;
  tableColumnsHidden: string[];
}

interface TableColumnSelectionState {

}

export class TableColumnSelection extends React.Component<TableColumnSelectionProps, TableColumnSelectionState> {

  constructor(props: TableColumnSelectionProps) {
    super(props);
    this.state = {

    };
  }

  render() {
    const { columns, onChange, tableColumnsHidden } = this.props;
    const checkboxes = <FormGroup>
      {
        columns.map(column => {
          return <Checkbox
            label={column}
            key={column}
            checked={!tableColumnsHidden.includes(column)}
            onChange={() => onChange(column)}
          />;
        })
      }
    </FormGroup>;
    return <Popover
      className={"table-column-selection"}
      content={checkboxes}
      position={Position.BOTTOM_RIGHT}
      inline
    >
      <Button rightIconName={IconNames.CARET_DOWN} text={"Columns"} />
    </Popover>;
  }
}
