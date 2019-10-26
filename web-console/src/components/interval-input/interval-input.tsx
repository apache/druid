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

import { Button, InputGroup, Popover, Position } from '@blueprintjs/core';
import { DateRange, DateRangePicker } from '@blueprintjs/datetime';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import './interval-input.scss';

const CURRENT_YEAR = new Date().getUTCFullYear();

export interface IntervalInputProps {
  interval?: string;
}

export interface IntervalInputState {
  currentInterval: string;
}

export class IntervalInput extends React.PureComponent<IntervalInputProps, IntervalInputState> {
  constructor(props: IntervalInputProps) {
    super(props);
    this.state = {
      currentInterval: this.props.interval
        ? this.props.interval
        : `${CURRENT_YEAR - 1}-01-01/${CURRENT_YEAR}-01-01`,
    };
  }

  parseInterval(interval: string): [Date | undefined, Date | undefined] | undefined {
    const dates = interval.split('/');
    if (dates.length !== 2) return;
    const startDate = new Date(dates[0]);
    const endDate = new Date(dates[1]);
    return [startDate, endDate];
  }
  render() {
    const { currentInterval } = this.state;
    console.log(currentInterval);
    return (
      <>
        <InputGroup
          value={`${currentInterval}`}
          className={'interval-input'}
          rightElement={
            <Popover
              content={
                <DateRangePicker
                  value={this.parseInterval(currentInterval)}
                  onChange={(selectedRange: DateRange) => {
                    const [selectedStart, selectedEnd] = selectedRange;
                    console.log(
                      selectedStart ? selectedStart.toISOString().substring(0, 10) : 'nooo ',
                    );
                    this.setState({
                      currentInterval:
                        selectedStart && selectedEnd
                          ? `${selectedStart
                              .toISOString()
                              .substring(0, 10)}/${selectedEnd.toISOString().substring(0, 10)}`
                          : currentInterval,
                    });
                  }}
                />
              }
              position={Position.BOTTOM_RIGHT}
            >
              <Button rightIcon={IconNames.CALENDAR} />
            </Popover>
          }
          onChange={(e: any) => this.setState({ currentInterval: e.target.value })}
        />
      </>
    );
  }
}
