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

import { Button, Collapse, TextArea } from '@blueprintjs/core';
import React from 'react';

interface JsonCollapseProps {
  stringValue: string;
  buttonText: string;
}

interface JsonCollapseState {
  isOpen: boolean;
}

export class JsonCollapse extends React.PureComponent<JsonCollapseProps, JsonCollapseState> {
  constructor(props: any) {
    super(props);
    this.state = {
      isOpen: false,
    };
  }

  render(): JSX.Element {
    const { stringValue, buttonText } = this.props;
    const { isOpen } = this.state;
    const prettyValue = JSON.stringify(JSON.parse(stringValue), undefined, 2);
    return (
      <div className="json-collapse">
        <Button
          minimal
          active={isOpen}
          onClick={() => this.setState({ isOpen: !isOpen })}
          text={buttonText}
        />
        <div>
          <Collapse isOpen={isOpen}>
            <TextArea readOnly value={prettyValue} />
          </Collapse>
        </div>
      </div>
    );
  }
}
