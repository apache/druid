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

import { Button, ButtonGroup, IButtonProps, Popover, Radio, RadioGroup } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { localStorageGet, LocalStorageKeys, localStorageSet } from '../../utils';

import './timed-button.scss';
import Timeout = NodeJS.Timeout;

export interface Interval {
  label: string;
  value: number;
}

export interface TimedButtonProps extends IButtonProps {
  intervals: Interval[];
  onRefresh: (auto: boolean) => void;
  localstoragekey?: LocalStorageKeys;
}

export interface TimedButtonState {
  interval: number;
}

export class TimedButton extends React.PureComponent<TimedButtonProps, TimedButtonState> {
  constructor(props: TimedButtonProps, context: any) {
    super(props, context);
    this.state = {
      interval: 0
    };
  }

  private timer: Timeout;

  componentDidMount(): void {
    if (this.props.localstoragekey) {
      this.setState({interval: Number(localStorageGet(this.props.localstoragekey))});
      this.timer = setTimeout(() => {
        this.continousRefresh();
      }, this.state.interval);
    }
  }

  continousRefresh = () => {
    this.props.onRefresh(true);
    if (this.state.interval) {
      this.timer = setTimeout(() => {
        this.continousRefresh();
      }, this.state.interval);
    }
  }

  handleSelection( selectedInterval: number) {
    clearTimeout(this.timer);
    this.setState({interval: selectedInterval});
    if (this.props.localstoragekey) {
      localStorageSet(this.props.localstoragekey, String(selectedInterval));
      this.continousRefresh();
    }
  }

  render() {
    const { intervals, localstoragekey, onRefresh, type, ...other } = this.props;
    const { interval } = this.state;

    return <ButtonGroup>
      <Button
        {...other}
        onClick={() => onRefresh(false)}
      />
      <Popover
        content={
          <RadioGroup
            className="refresh-options"
            onChange={value => this.handleSelection(Number(value.currentTarget.value))}
            selectedValue={interval}
          >
            {intervals.map((interval: any) => (
              <Radio
                label={interval.label}
                value={interval.value}
                key={interval}
              />
            ))}
          </RadioGroup>
        }
      >
        <Button
          rightIcon={IconNames.CARET_DOWN}
        />
      </Popover>
    </ButtonGroup>;
  }
}
