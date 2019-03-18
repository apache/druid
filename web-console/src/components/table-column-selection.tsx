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
  tableName: string;
  onChange: () => void;
}

interface TableColumnSelectionState {
  tableColumnSelection: any;
}

export class TableColumnSelection extends React.Component<TableColumnSelectionProps, TableColumnSelectionState> {

  constructor(props: TableColumnSelectionProps) {
    super(props);
    this.state = {
      tableColumnSelection: JSON.parse(localStorage[this.props.tableName])
    };
  }

  changeTableColumnSelection(column: string) {
    const { tableName, onChange } = this.props;
    const { tableColumnSelection } = this.state;
    const newSelection = Object.assign({}, tableColumnSelection, {[column]: !tableColumnSelection[column]});
    localStorage.setItem(tableName, JSON.stringify(newSelection));
    this.setState({
      tableColumnSelection: newSelection
    });
    onChange();
  }

  render() {
    const { tableColumnSelection } = this.state;
    const columns = Object.keys(tableColumnSelection);
    const checkboxes = <FormGroup>
      {
        columns.map(column => {
          return <Checkbox
            label={column}
            key={column}
            checked={tableColumnSelection[column]}
            onChange={() => this.changeTableColumnSelection(column)}
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
      <Button text={"Hide columns"} iconName={IconNames.REMOVE_COLUMN_LEFT}/>
    </Popover>;
  }
}
