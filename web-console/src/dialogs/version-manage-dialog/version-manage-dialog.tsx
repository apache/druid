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

import { Dialog, Popover, Radio, RadioGroup } from '@blueprintjs/core';
import { DateInput } from '@blueprintjs/datetime';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import React from 'react';
import ReactTable, { CellInfo } from 'react-table';

import { ActionIcon } from '../../components/action-icon/action-icon';
import { QueryManager } from '../../utils';

import './version-manage-dialog.scss';
// tslint:disable-next-line:ordered-imports
import '@blueprintjs/datetime/lib/css/blueprint-datetime.css';

export interface VersionManageDialogProps {
  datasource: string;
  onClose: () => void;
}

export interface VersionManageDialogState {
  segments: IMergeSegment[] | null;
  segmentLoading: boolean;
}

interface ISegment {
  id: string;
  start: string;
  end: string;
  version: string;
  used: boolean;
}

interface IMergeSegment {
  start: string;
  end: string;
  currentVersion: string;
  versions: string[];
  segments: ISegment[];
}

export class VersionManageDialog extends React.PureComponent<
  VersionManageDialogProps,
  VersionManageDialogState
> {
  private segmentsSqlQueryManager: QueryManager<string, ISegment[]>;

  private segmentDate: Date = new Date();

  constructor(props: VersionManageDialogProps) {
    super(props);

    this.state = {
      segments: [],
      segmentLoading: false,
    };

    this.segmentsSqlQueryManager = new QueryManager({
      processQuery: async (query: string) => {
        this.setState({
          segmentLoading: true,
        });
        const segmentResp = await axios.post(
          `/druid/coordinator/v1/metadata/datasources/${this.props.datasource}/segments?full`,
          [query],
        );
        const unusedSegmentResp = await axios.post(
          `/druid/coordinator/v1/metadata/datasources/${this.props.datasource}/unusedSegments?full`,
          [query],
        );
        const segments: ISegment[] = segmentResp.data.map(
          (segment: { interval: string; version: string; identifier: string }) => ({
            start: segment.interval.split('/')[0],
            end: segment.interval.split('/')[1],
            version: segment.version,
            used: true,
            id: segment.identifier,
          }),
        );
        unusedSegmentResp.data.forEach(
          (segment: { interval: string; version: any; identifier: string }) =>
            segments.push({
              start: segment.interval.split('/')[0],
              end: segment.interval.split('/')[1],
              version: segment.version,
              used: false,
              id: segment.identifier,
            }),
        );
        return segments;
      },
      onStateChange: ({ result }) => {
        this.setState({
          segments: result ? this.mergeSegmentResult(result!) : [],
          segmentLoading: false,
        });
      },
    });
  }

  componentDidMount() {
    this.segmentsSqlQueryManager.runQuery(this.buildInterval(this.segmentDate));
  }

  render() {
    const { datasource } = this.props;

    const columns = [
      {
        Header: 'start',
        accessor: 'start',
      },
      {
        Header: 'end',
        accessor: 'end',
      },
      {
        accessor: 'currentVersion',
        Header: 'currentVersion',
        Cell: (row: CellInfo) => {
          return (
            <div>
              <span>{row.value}</span>
              <Popover
                content={
                  <RadioGroup
                    label={'all version'}
                    className="timed-button"
                    selectedValue={row.value}
                    onChange={e => this.onChangeSegmentVersion(row.original, e.currentTarget.value)}
                  >
                    {row.original.versions &&
                      row.original.versions.map((version: string) => (
                        <Radio label={version} value={version} key={version} />
                      ))}
                  </RadioGroup>
                }
              >
                <ActionIcon icon={IconNames.EDIT} />
              </Popover>
            </div>
          );
        },
      },
    ];

    return (
      <Dialog
        className="retention-dialog"
        isOpen
        title={`Segment version manage: ${datasource}`}
        onClose={this.props.onClose}
      >
        <div className="segment-table">
          <div className="segment-table-date">
            <span className="segment-table-date-text">Date</span>
            <DateInput
              formatDate={date => date.toLocaleDateString()}
              parseDate={str => new Date(str)}
              canClearSelection={false}
              value={this.segmentDate}
              onChange={value => this.onSegmentDateChange(value)}
            />
          </div>
          <ReactTable
            data={this.state.segments ? this.state.segments : []}
            columns={columns}
            loading={this.state.segmentLoading}
            noDataText={'no segments'}
            showPagination
          />
        </div>
      </Dialog>
    );
  }

  async onChangeSegmentVersion(row: IMergeSegment, version: string) {
    // markUnused
    await axios.post(`/druid/coordinator/v1/datasources/${this.props.datasource}/markUnused`, {
      segmentIds: row.segments.filter(segment => segment.used).map(segment => segment.id),
    });
    // markUsed
    await axios.post(`/druid/coordinator/v1/datasources/${this.props.datasource}/markUsed`, {
      segmentIds: row.segments
        .filter(segment => segment.version === version)
        .map(segment => segment.id),
    });
    this.segmentsSqlQueryManager.runQuery(this.buildInterval(this.segmentDate));
  }

  onSegmentDateChange(segmentDate: Date) {
    this.segmentsSqlQueryManager.runQuery(this.buildInterval(segmentDate));
    this.segmentDate = segmentDate;
  }

  mergeSegmentResult(result: ISegment[]): IMergeSegment[] {
    const segmentGroup: Map<string, ISegment[]> = new Map<string, ISegment[]>();
    for (const segment of result) {
      const key = `${segment.start}-${segment.end}`;
      if (!segmentGroup.get(key)) {
        segmentGroup.set(key, []);
      }
      segmentGroup.get(key)!.push(segment);
    }
    const mergeResult: IMergeSegment[] = [];
    segmentGroup.forEach(group => {
      const currentSegment = group!.filter(segment => segment.used);
      const versions: string[] = [];
      group!
        .map((segment: ISegment) => segment.version)
        .forEach(
          (version: string) =>
            versions.filter(v => v === version).length === 0 && versions.push(version),
        );
      mergeResult.push({
        start: group![0].start,
        end: group![0].end,
        currentVersion: currentSegment.length > 0 ? currentSegment[0].version : '',
        versions,
        segments: group,
      });
    });
    return mergeResult;
  }

  buildInterval(segmentDate: Date): string {
    const start = `${segmentDate.getFullYear()}-${(segmentDate.getMonth() + 1)
      .toString()
      .padStart(2, '0')}-${segmentDate
      .getDate()
      .toString()
      .padStart(2, '0')}T00:00:00`;
    const endDate = new Date(
      segmentDate.getFullYear(),
      segmentDate.getMonth(),
      segmentDate.getDate() + 1,
    );
    const end = `${endDate.getFullYear()}-${(endDate.getMonth() + 1)
      .toString()
      .padStart(2, '0')}-${endDate
      .getDate()
      .toString()
      .padStart(2, '0')}T00:00:00`;
    return `${start}/${end}`;
  }
}
