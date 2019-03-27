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

import { Dialog } from "@blueprintjs/core";
import * as React from "react";

import { Card, JSONCollapse } from "../components/filler";

import "./history-dialog.scss";

interface HistoryDialogProps extends React.Props<any> {
  historyRecords: any;
}

interface HistoryDialogState {

}

export class HistoryDialog extends React.Component<HistoryDialogProps, HistoryDialogState> {
  constructor(props: HistoryDialogProps) {
    super(props);
    this.state = {

    };
  }

  renderRecords() {
    const {children, historyRecords} = this.props;
    let content;
    if (historyRecords.length === 0) {
      content = <div className={"no-record"}>No history records available</div>;
    } else {
       content = <>
          <h3>History</h3>
          <div className={"history-record-entries"}>
            {
              historyRecords.map((record: any) => {
                const auditInfo = record.auditInfo;
                const auditTime = record.auditTime;
                const formattedTime = auditTime.replace("T", " ").substring(0, auditTime.length - 5);

                return <div key={record.auditTime} className={"history-record-entry"}>
                  <Card>
                    <div className={"history-record-title"}>
                      <h5>Change</h5>
                      <p>{formattedTime}</p>
                    </div>
                    <hr/>
                    <p>{auditInfo.comment === "" ? "(No comment)" : auditInfo.comment}</p>
                    <JSONCollapse
                      stringValue={record.payload}
                      buttonText={"Payload"}
                    />
                  </Card>
                </div>;
              })
            }
          </div>
         </>;
    }
    return <div className={"history-record-container"}>
      {content}
      {children}
    </div>;
  }

  render(): React.ReactNode {
    return <Dialog
      isOpen
      {...this.props}
      className={"history-dialog"}
    >
      {this.renderRecords()}
    </Dialog>;
  }
}
