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

import { IntermediateQueryState } from './intermediate-query-state';
import type { QueryManagerOptions } from './query-manager';
import { QueryManager } from './query-manager';
import { QueryState } from './query-state';
import { ResultWithAuxiliaryWork } from './result-with-auxiliary-work';

// jsdom's AbortSignal (used as the global in the jest environment) predates `throwIfAborted`,
// which the QueryManager relies on. Polyfill it so the tests exercise real browser behavior.
beforeAll(() => {
  if (typeof AbortSignal.prototype.throwIfAborted !== 'function') {
    AbortSignal.prototype.throwIfAborted = function throwIfAborted(this: AbortSignal) {
      if (this.aborted) throw (this as any).reason ?? new Error('Aborted');
    };
  }
});

// All debounces and background delays are set to 0 so that `run()` executes synchronously
// (the constructor wires the runWhen* helpers directly to `run` when the debounce is 0, and
// every `wait(delay)` is guarded by `if (delay)`). That makes the manager fully driven by the
// microtask queue, which we flush with `Promise.resolve()`.
const ZERO_TIMING = {
  debounceInit: 0,
  debounceIdle: 0,
  debounceLoading: 0,
  backgroundStatusCheckInitDelay: 0,
  backgroundStatusCheckDelay: 0,
} as const;

// Promise-returning helpers keep the test callbacks free of unnecessary `async` (which the
// require-await lint rule rejects) while still satisfying the Promise-returning signatures.
const resolved = <T>(value: T): Promise<T> => Promise.resolve(value);
const rejected = (error: Error): Promise<never> => Promise.reject(error);

interface Harness<Q, R, I = never, E extends Error = Error> {
  manager: QueryManager<Q, R, I, E>;
  states: QueryState<R, E, I>[];
}

function makeManager<Q, R, I = never, E extends Error = Error>(
  options: Omit<QueryManagerOptions<Q, R, I, E>, 'onStateChange'> &
    Partial<Pick<QueryManagerOptions<Q, R, I, E>, 'onStateChange'>>,
): Harness<Q, R, I, E> {
  const states: QueryState<R, E, I>[] = [];
  const manager = new QueryManager<Q, R, I, E>({
    ...ZERO_TIMING,
    ...options,
    onStateChange: state => states.push(state),
  });
  return { manager, states };
}

// Flushes microtasks until the latest recorded state past `fromIndex` is settled (data without
// auxiliaryLoading, or an error). Returns that final state.
async function settleAfter<R, E extends Error, I>(
  states: QueryState<R, E, I>[],
  fromIndex: number,
): Promise<QueryState<R, E, I>> {
  for (let i = 0; i < 500; i++) {
    await Promise.resolve();
    const last = states[states.length - 1];
    if (
      states.length > fromIndex &&
      last &&
      (last.isError() || (last.state === 'data' && !last.auxiliaryLoading))
    ) {
      return last;
    }
  }
  throw new Error('query manager did not settle');
}

describe('QueryManager', () => {
  describe('basic query lifecycle', () => {
    it('transitions from loading to data', async () => {
      const { manager, states } = makeManager<string, string>({
        processQuery: query => resolved(`result:${query}`),
      });

      manager.runQuery('hello');
      const final = await settleAfter(states, 0);

      expect(states[0].isLoading()).toEqual(true);
      expect(final.state).toEqual('data');
      expect(final.data).toEqual('result:hello');
      expect(manager.getState().data).toEqual('result:hello');
      expect(manager.getLastQuery()).toEqual('hello');
    });

    it('passes the query through to processQuery', async () => {
      const seen: string[] = [];
      const { manager, states } = makeManager<string, number>({
        processQuery: query => {
          seen.push(query);
          return resolved(query.length);
        },
      });

      manager.runQuery('abcd');
      await settleAfter(states, 0);

      expect(seen).toEqual(['abcd']);
      expect(manager.getState().data).toEqual(4);
    });

    it('captures errors thrown by processQuery into an error state', async () => {
      const { manager, states } = makeManager<string, string>({
        processQuery: () => rejected(new Error('boom')),
      });

      manager.runQuery('x');
      const final = await settleAfter(states, 0);

      expect(final.isError()).toEqual(true);
      expect(final.getErrorMessage()).toEqual('boom');
    });

    it('keeps prior data as lastData on a subsequent run', async () => {
      const { manager, states } = makeManager<string, string>({
        processQuery: query => resolved(`result:${query}`),
      });

      manager.runQuery('one');
      await settleAfter(states, 0);

      const start = states.length;
      manager.runQuery('two');
      // While loading the second query, the previous data is exposed as lastData.
      expect(states[start].isLoading()).toEqual(true);
      expect(states[start].lastData).toEqual('result:one');

      const final = await settleAfter(states, start);
      expect(final.data).toEqual('result:two');
    });
  });

  describe('query superseding', () => {
    it('keeps only the newest query result when a second query is issued mid-flight', async () => {
      let resolveFirst: ((value: string) => void) | undefined;
      const { manager, states } = makeManager<string, string>({
        processQuery: query =>
          query === 'first'
            ? new Promise<string>(resolve => {
                resolveFirst = resolve;
              })
            : Promise.resolve(`result:${query}`),
      });

      manager.runQuery('first');
      manager.runQuery('second');

      const final = await settleAfter(states, 0);
      expect(final.data).toEqual('result:second');

      // Even if the stale first query resolves later, it must not overwrite the state.
      resolveFirst?.('result:first');
      await Promise.resolve();
      await Promise.resolve();
      expect(manager.getState().data).toEqual('result:second');
    });
  });

  describe('intermediate query state (backgroundStatusCheck)', () => {
    it('loops through intermediate states until a final result is produced', async () => {
      // delay: 0 on the IntermediateQueryState skips the real setTimeout wait (the
      // backgroundStatusCheck*Delay defaults can't be zeroed since `0 || 500` is 500).
      const { manager, states } = makeManager<string, string, string>({
        processQuery: () => resolved(new IntermediateQueryState('pending', 0)),
        backgroundStatusCheck: state =>
          resolved(state === 'pending' ? new IntermediateQueryState('almost', 0) : 'done'),
      });

      manager.runQuery('go');
      const final = await settleAfter(states, 0);

      const intermediates = states
        .filter(s => s.isLoading())
        .map(s => s.intermediate)
        .filter(Boolean);
      expect(intermediates).toEqual(['pending', 'almost']);
      expect(final.data).toEqual('done');
    });

    it('errors when an intermediate state is returned without a backgroundStatusCheck', async () => {
      const { manager, states } = makeManager<string, string, string>({
        processQuery: () => resolved(new IntermediateQueryState('pending')),
      });

      manager.runQuery('go');
      const final = await settleAfter(states, 0);

      expect(final.isError()).toEqual(true);
      expect(final.getErrorMessage()).toContain('backgroundStatusCheck must be set');
    });

    it('swallows recoverable background errors and surfaces them as intermediateError', async () => {
      let checks = 0;
      const { manager, states } = makeManager<string, string, string>({
        processQuery: () => resolved(new IntermediateQueryState('pending', 0)),
        backgroundStatusCheck: () => {
          checks++;
          return checks === 1 ? rejected(new Error('transient')) : resolved('done');
        },
        swallowBackgroundError: e => e.message === 'transient',
      });

      manager.runQuery('go');
      const final = await settleAfter(states, 0);

      const withError = states.find(s => s.intermediateError);
      expect(withError?.intermediateError?.message).toEqual('transient');
      expect(final.data).toEqual('done');
    });
  });

  describe('ResultWithAuxiliaryWork', () => {
    interface Enriched {
      base: number;
      a?: number;
      b?: number;
    }

    const makeAuxProcessQuery = () => (): Promise<ResultWithAuxiliaryWork<Enriched>> =>
      resolved(
        new ResultWithAuxiliaryWork<Enriched>({ base: 1 }, [
          result => resolved({ ...result, a: 2 }),
          result => resolved({ ...result, b: 3 }),
        ]),
      );

    it('unwraps a result that has no auxiliary queries', async () => {
      const { manager, states } = makeManager<string, Enriched>({
        processQuery: () => resolved(new ResultWithAuxiliaryWork<Enriched>({ base: 1 }, [])),
      });

      manager.runQuery('go');
      const final = await settleAfter(states, 0);

      expect(final.data).toEqual({ base: 1 });
      expect(states.some(s => s.auxiliaryLoading)).toEqual(false);
    });

    it('enriches incrementally on a foreground run', async () => {
      const { manager, states } = makeManager<string, Enriched>({
        processQuery: makeAuxProcessQuery(),
      });

      manager.runQuery('go');
      const final = await settleAfter(states, 0);

      // The base result and the first auxiliary result are published with auxiliaryLoading: true.
      const auxStates = states.filter(s => s.auxiliaryLoading);
      expect(auxStates.map(s => s.data)).toEqual([{ base: 1 }, { base: 1, a: 2 }]);

      // The final state is fully enriched and no longer auxiliary-loading.
      expect(final.auxiliaryLoading).toBeFalsy();
      expect(final.data).toEqual({ base: 1, a: 2, b: 3 });
    });

    it('does NOT enrich incrementally on a background run', async () => {
      const { manager, states } = makeManager<string, Enriched>({
        processQuery: makeAuxProcessQuery(),
      });

      // Initial foreground run so there is a lastQuery to rerun.
      manager.runQuery('go');
      await settleAfter(states, 0);

      const start = states.length;
      manager.rerunLastQuery(true);
      const final = await settleAfter(states, start);

      // The background run emits exactly one state: the final, fully enriched, non-loading result.
      const backgroundStates = states.slice(start);
      expect(backgroundStates).toHaveLength(1);
      expect(backgroundStates[0].auxiliaryLoading).toBeFalsy();
      expect(final.data).toEqual({ base: 1, a: 2, b: 3 });
    });

    it('enriches incrementally on a foreground rerun', async () => {
      const { manager, states } = makeManager<string, Enriched>({
        processQuery: makeAuxProcessQuery(),
      });

      manager.runQuery('go');
      await settleAfter(states, 0);

      const start = states.length;
      manager.rerunLastQuery(); // foreground (default)
      await settleAfter(states, start);

      const auxStates = states.slice(start).filter(s => s.auxiliaryLoading);
      expect(auxStates.map(s => s.data)).toEqual([{ base: 1 }, { base: 1, a: 2 }]);
    });
  });

  describe('intermediate query tracking', () => {
    it('records the last intermediate query set via the extra callback', async () => {
      const { manager, states } = makeManager<string, string>({
        processQuery: (query, _signal, extra) => {
          extra.setIntermediateQuery(`intermediate:${query}`);
          return resolved(`result:${query}`);
        },
      });

      manager.runQuery('go');
      await settleAfter(states, 0);

      expect(manager.getLastIntermediateQuery()).toEqual('intermediate:go');
    });
  });

  describe('control methods', () => {
    it('reset returns the manager to the init state', async () => {
      const { manager, states } = makeManager<string, string>({
        processQuery: query => resolved(`result:${query}`),
      });

      manager.runQuery('go');
      await settleAfter(states, 0);

      manager.reset();
      expect(manager.getState().isInit()).toEqual(true);
    });

    it('terminate stops further state changes and ignores new queries', async () => {
      const { manager, states } = makeManager<string, string>({
        processQuery: query => resolved(`result:${query}`),
      });

      manager.runQuery('go');
      await settleAfter(states, 0);

      const countBefore = states.length;
      manager.terminate();
      expect(manager.isTerminated()).toEqual(true);

      manager.runQuery('again');
      await Promise.resolve();
      await Promise.resolve();
      expect(states.length).toEqual(countBefore);
    });

    it('initialises from a provided initState', () => {
      const { manager } = makeManager<string, string>({
        initState: new QueryState<string>({ data: 'seed' }),
        processQuery: query => resolved(`result:${query}`),
      });

      expect(manager.getState().data).toEqual('seed');
    });
  });
});
