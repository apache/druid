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

import { FormGroup, NumericInput, Slider } from '@blueprintjs/core';
import type { ECharts } from 'echarts';
import * as echarts from 'echarts';
import React, { useEffect, useMemo, useRef, useState } from 'react';

import { Loader } from '../../../components/loader/loader';
import { useQueryManager } from '../../../hooks';
import { Api } from '../../../singletons';

import './auto-scaler-panel.scss';

interface AutoScalerRow {
  lag: number;
  taskCount: number;
}

interface AutoScalerPanelProps {
  supervisorId: string;
}

export function getAutoScalerValidationError({
  taskCountMin,
  taskCountMax,
  maxProcessingRatePerTask,
  optimalTaskIdleRatio,
  criticalLag,
  currentTaskCount,
}: {
  taskCountMin: number;
  taskCountMax: number;
  maxProcessingRatePerTask: number;
  optimalTaskIdleRatio: number;
  criticalLag: number;
  currentTaskCount: number | undefined;
}): string | undefined {
  if (taskCountMin > taskCountMax) return 'Minimum task count must not exceed maximum task count';
  if (maxProcessingRatePerTask < 100) return 'Max processing rate / task must be at least 100';
  if (optimalTaskIdleRatio <= 0 || optimalTaskIdleRatio >= 1) {
    return 'Optimal task idle ratio must be greater than 0 and less than 1';
  }
  if (criticalLag < 1000) return 'Critical lag must be at least 1000';
  if (
    currentTaskCount !== undefined &&
    (currentTaskCount < taskCountMin || currentTaskCount > taskCountMax)
  ) {
    return 'Current task count must be within the minimum and maximum task count';
  }
  return undefined;
}

export const AutoScalerPanel = React.memo(function AutoScalerPanel(props: AutoScalerPanelProps) {
  const { supervisorId } = props;

  const [taskCountMin, setTaskCountMin] = useState<number>(1);
  const [taskCountMax, setTaskCountMax] = useState<number>(10);
  const [maxProcessingRatePerTask, setMaxProcessingRatePerTask] = useState<number>(10000);
  const [optimalTaskIdleRatio, setOptimalTaskIdleRatio] = useState<number>(0.2);
  const [lagWeight, setLagWeight] = useState<number>(0.4);
  // Idle weight is the complement of lag weight; one slider drives both.
  const idleWeight = Math.round((1 - lagWeight) * 10) / 10;
  const [criticalLag, setCriticalLag] = useState<number>(100000);
  // Undefined means "let the server use the supervisor's live task count".
  const [currentTaskCount, setCurrentTaskCount] = useState<number | undefined>(undefined);

  const chartContainerRef = useRef<HTMLDivElement | undefined>(undefined);
  const chartRef = useRef<ECharts | undefined>(undefined);
  const query = useMemo(
    () => ({
      supervisorId,
      taskCountMin,
      taskCountMax,
      maxProcessingRatePerTask,
      optimalTaskIdleRatio,
      lagWeight,
      idleWeight,
      criticalLag,
      currentTaskCount,
    }),
    [
      supervisorId,
      taskCountMin,
      taskCountMax,
      maxProcessingRatePerTask,
      optimalTaskIdleRatio,
      lagWeight,
      idleWeight,
      criticalLag,
      currentTaskCount,
    ],
  );
  const validationError = getAutoScalerValidationError(query);

  const [dataState] = useQueryManager<
    {
      supervisorId: string;
      taskCountMin: number;
      taskCountMax: number;
      maxProcessingRatePerTask: number;
      optimalTaskIdleRatio: number;
      lagWeight: number;
      idleWeight: number;
      criticalLag: number;
      currentTaskCount: number | undefined;
    },
    AutoScalerRow[]
  >({
    query: validationError ? undefined : query,
    debounceIdle: 300,
    debounceLoading: 500,
    processQuery: async (params, signal) => {
      const resp = await Api.instance.get<{ data: AutoScalerRow[] }>(
        `/druid/indexer/v1/supervisor/${Api.encodePath(params.supervisorId)}/autoscaler`,
        {
          params: {
            taskCountMin: params.taskCountMin,
            taskCountMax: params.taskCountMax,
            maxProcessingRatePerTask: params.maxProcessingRatePerTask,
            optimalTaskIdleRatio: params.optimalTaskIdleRatio,
            lagWeight: params.lagWeight,
            idleWeight: params.idleWeight,
            criticalLag: params.criticalLag,
            currentTaskCount: params.currentTaskCount,
          },
          signal,
        },
      );
      return resp.data.data ?? (resp.data as any);
    },
  });

  function setupChart(container: HTMLDivElement): ECharts {
    const myChart = echarts.init(container, 'dark');
    myChart.setOption({
      tooltip: {
        trigger: 'axis',
      },
      grid: {
        left: '3%',
        right: '4%',
        bottom: '3%',
        containLabel: true,
      },
      xAxis: {
        type: 'value',
        name: 'Lag (records)',
        nameLocation: 'middle',
        nameGap: 30,
      },
      yAxis: {
        type: 'value',
        name: 'Task count',
        nameLocation: 'middle',
        nameGap: 40,
      },
      series: [
        {
          name: 'Task count',
          type: 'line',
          showSymbol: false,
          data: [],
        },
      ],
    });
    return myChart;
  }

  useEffect(() => {
    return () => {
      chartRef.current?.dispose();
    };
  }, []);

  useEffect(() => {
    const myChart = chartRef.current;
    const data = dataState.data;
    if (!myChart || !data) return;

    myChart.setOption({
      series: [
        {
          data: data.map(row => [row.lag, row.taskCount]),
        },
      ],
    });
  }, [dataState.data]);

  useEffect(() => {
    const myChart = chartRef.current;
    if (!myChart) return;
    myChart.resize();
  }, []);

  const errorMessage = validationError ?? dataState.getErrorMessage();

  return (
    <div className="auto-scaler-panel">
      <div className="auto-scaler-controls">
        <FormGroup label="Min task count" inline>
          <NumericInput
            value={taskCountMin}
            min={1}
            max={taskCountMax}
            onValueChange={v => setTaskCountMin(v)}
            buttonPosition="none"
            fill
          />
        </FormGroup>
        <FormGroup label="Max task count" inline>
          <NumericInput
            value={taskCountMax}
            min={taskCountMin}
            onValueChange={v => setTaskCountMax(v)}
            buttonPosition="none"
            fill
          />
        </FormGroup>
        <FormGroup label="Max processing rate / task" inline>
          <NumericInput
            value={maxProcessingRatePerTask}
            min={100}
            onValueChange={v => setMaxProcessingRatePerTask(v)}
            buttonPosition="none"
            fill
          />
        </FormGroup>
        <FormGroup label="Optimal task idle ratio" inline>
          <NumericInput
            value={optimalTaskIdleRatio}
            min={0.01}
            max={0.99}
            stepSize={0.1}
            minorStepSize={0.01}
            onValueChange={v => setOptimalTaskIdleRatio(v)}
            fill
          />
        </FormGroup>
        <FormGroup label="Current task count" inline>
          <NumericInput
            value={currentTaskCount ?? ''}
            min={taskCountMin}
            max={taskCountMax}
            placeholder="Supervisor's current"
            onValueChange={v => setCurrentTaskCount(isNaN(v) ? undefined : v)}
            buttonPosition="none"
            fill
          />
        </FormGroup>
        <FormGroup label="Critical lag (records)" inline>
          <NumericInput
            value={criticalLag}
            min={1000}
            onValueChange={v => setCriticalLag(v)}
            buttonPosition="none"
            fill
          />
        </FormGroup>
        <FormGroup label={`Lag ${lagWeight.toFixed(1)} / Idle ${idleWeight.toFixed(1)} weight`}>
          <Slider
            min={0}
            max={1}
            stepSize={0.1}
            labelStepSize={0.5}
            value={lagWeight}
            onChange={v => setLagWeight(Math.round(v * 10) / 10)}
          />
        </FormGroup>
      </div>
      <div className="auto-scaler-chart-area">
        {errorMessage && <div className="auto-scaler-error">{errorMessage}</div>}
        {dataState.loading && <Loader />}
        <div
          className="auto-scaler-echart"
          ref={container => {
            if (chartRef.current || !container) return;
            chartContainerRef.current = container;
            chartRef.current = setupChart(container);
          }}
        />
      </div>
    </div>
  );
});
