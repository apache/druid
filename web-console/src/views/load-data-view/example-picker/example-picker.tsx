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

import { Button, Callout, FormGroup, HTMLSelect, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { ExampleManifest } from '../../../utils/sampler';

export interface ExamplePickerProps {
  exampleManifests: ExampleManifest[];
  onSelectExample: (exampleManifest: ExampleManifest) => void;
}

export interface ExamplePickerState {
  selectedIndex: number;
}

export class ExamplePicker extends React.PureComponent<ExamplePickerProps, ExamplePickerState> {
  constructor(props: ExamplePickerProps, context: any) {
    super(props, context);
    this.state = {
      selectedIndex: 0,
    };
  }

  render(): JSX.Element {
    const { exampleManifests, onSelectExample } = this.props;
    const { selectedIndex } = this.state;

    return (
      <>
        <FormGroup label="Select example dataset">
          <HTMLSelect
            fill
            value={selectedIndex}
            onChange={e => this.setState({ selectedIndex: e.target.value as any })}
          >
            {exampleManifests.map((exampleManifest, i) => (
              <option key={i} value={i}>
                {exampleManifest.name}
              </option>
            ))}
          </HTMLSelect>
        </FormGroup>
        <FormGroup>
          <Callout>{exampleManifests[selectedIndex].description}</Callout>
        </FormGroup>
        <FormGroup>
          <Button
            text="Load example"
            rightIcon={IconNames.ARROW_RIGHT}
            intent={Intent.PRIMARY}
            onClick={() => {
              onSelectExample(exampleManifests[selectedIndex]);
            }}
          />
        </FormGroup>
      </>
    );
  }
}
