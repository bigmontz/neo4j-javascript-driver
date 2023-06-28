/**
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Connection from "./connection"
import ConnectionProvider from "./connection-provider"
import { PROTOCOL_ERROR, newError } from "./error"
import { ConnectionHolder } from "./internal/connection-holder"
import { ACCESS_MODE_READ } from "./internal/constants"
import { ResultStreamObserver } from "./internal/observers"
import Record from "./record"
import { DEFAULT_ON_COMPLETED, DEFAULT_ON_ERROR, DEFAULT_ON_KEYS, ResultObserver } from "./result"
import ResultSummary from "./result-summary"
import { Readable } from 'stream'

export interface OpenCdcStreamingResultConfig {
  database: string, 
  from: string,
  connectionProvider: ConnectionProvider
}

interface QueuedResultObserver extends ResultObserver {
  dequeue: () => Promise<IteratorResult<Record, ResultSummary>>
  dequeueUntilDone: () => Promise<IteratorResult<Record, ResultSummary>>
  head: () => Promise<IteratorResult<Record, ResultSummary>>
  size: number
}


export default class CdcStreamingResult {

  static async open ({
    database,
    from,
    connectionProvider
  }: OpenCdcStreamingResultConfig): Promise<CdcStreamingResult> {

    const connectionHolder = new ConnectionHolder({
      mode: ACCESS_MODE_READ,
      database,
      connectionProvider
    })

    if (connectionHolder.initializeConnection()) {
      const connection = await connectionHolder.getConnection() as Connection
      const observer = connection.protocol().beginStreaming({
        from
      }, {})

      return new CdcStreamingResult(
        observer,
        from,
        connectionHolder.close.bind(connectionHolder)
      ) 
    } else {
      throw new Error('Cant acquire connection to open streaming')
    }
  }

  private _completionPromise?: Promise<void>
  private _resolveCompletionPromise?: () => void
  private _rejectCompletionPromise?: (e: Error) => void
  private _closed: boolean

  private constructor(
    private _observer: ResultStreamObserver,  
    private _currentChangeIdentifier: string,
    private _releaseConnection: () => Promise<void>,
    
  ) {
    this._closed = false
  }

  public subscribe (observer: ResultObserver): void {
    if (this._completionPromise) {
      throw new Error('already subscribed')
    }
    const DEFAULT_ON_NEXT = (record: Record) => {}

    const onCompletedOriginal = observer.onCompleted ?? DEFAULT_ON_COMPLETED
    const onErrorOriginal = observer.onError ?? DEFAULT_ON_ERROR
    const onKeysOriginal = observer.onKeys ?? DEFAULT_ON_KEYS
    const onNextOriginal = observer.onNext ?? DEFAULT_ON_NEXT

    this._completionPromise = new Promise((resolve, reject)  => {
      this._resolveCompletionPromise = resolve
      this._rejectCompletionPromise = reject
    })

    const onCompletedWrapper = (metadata: any): void => {
      this._resolveCompletionPromise?.call(this._resolveCompletionPromise)
      return onCompletedOriginal.call(observer, new ResultSummary('not query', {}, {}))
    }

    const onErrorWrapper = (error: Error): void => {
      this._rejectCompletionPromise?.call(this._rejectCompletionPromise)
      onErrorOriginal.call(observer, error)
    }

    const onNextWrapper = (record: Record): void => {
      if (this._closed) {
        return
      }
      // @ts-expect-error
      this._currentChangeIdentifier = record._fields[0]
      onNextOriginal.call(observer, record)
    }

    this._observer.subscribe({
      onNext: onNextWrapper,
      onKeys: onKeysOriginal,
      onCompleted: onCompletedWrapper,
      onError: onErrorWrapper
    })
  }

  public toReadableStream () {
    return Readable.from(this)
  }

  public consume (cb: (r: Record) => void): Promise<void> {
    this.subscribe({
      onNext: cb
    })

    return this._completionPromise!
  }

  [Symbol.asyncIterator](): AsyncIterator<Record, undefined>  {
    const state: {
      finished: boolean
      queuedObserver?: QueuedResultObserver
    } = { finished: false }


    const initializeObserver = async (): Promise<QueuedResultObserver> => {
      if (state.queuedObserver === undefined) {
        state.queuedObserver = this._createQueuedResultObserver(() => {})
        this.subscribe(state.queuedObserver)
      }
      return state.queuedObserver
    }

    return {
      next: async () => {
        if (state.finished) {
          return { done: true, value: undefined }
        }
        const queuedObserver = await initializeObserver()
        const next = await queuedObserver.dequeue()
        if (next.done === true) {
          state.finished = next.done
          return {
            done: true
          }
        }
        return next
      },
      return: async () => {
        if (state.finished) {
          return { done: true }
        }

        if (this._closed) {
          await this.close()
        }
      
        const queuedObserver = await initializeObserver()
        const last = await queuedObserver.dequeueUntilDone()
        state.finished = true
        if (last.done === true) {
          return {
            done: true
          }
        }
        return {
          done: false,
          value: last.value
        }
      }
    }
    
  }


  private _createQueuedResultObserver (onQueueSizeChanged: () => void): QueuedResultObserver {
    interface ResolvablePromise<T> {
      promise: Promise<T>
      resolve: (arg: T) => any | undefined
      reject: (arg: Error) => any | undefined
    }

    function createResolvablePromise (): ResolvablePromise<IteratorResult<Record, ResultSummary>> {
      const resolvablePromise: any = {}
      resolvablePromise.promise = new Promise((resolve, reject) => {
        resolvablePromise.resolve = resolve
        resolvablePromise.reject = reject
      })
      return resolvablePromise
    }

    type QueuedResultElementOrError = IteratorResult<Record, ResultSummary> | Error

    function isError (elementOrError: QueuedResultElementOrError): elementOrError is Error {
      return elementOrError instanceof Error
    }

    async function dequeue (): Promise<IteratorResult<Record, ResultSummary>> {
      if (buffer.length > 0) {
        const element = buffer.shift() ?? newError('Unexpected empty buffer', PROTOCOL_ERROR)
        onQueueSizeChanged()
        if (isError(element)) {
          throw element
        }
        return element
      }
      promiseHolder.resolvable = createResolvablePromise()
      return await promiseHolder.resolvable.promise
    }

    const buffer: QueuedResultElementOrError[] = []
    const promiseHolder: {
      resolvable: ResolvablePromise<IteratorResult<Record, ResultSummary>> | null
    } = { resolvable: null }

    const observer = {
      onNext: (record: Record) => {
        observer._push({ done: false, value: record })
      },
      onCompleted: (summary: ResultSummary) => {
        observer._push({ done: true, value: summary })
      },
      onError: (error: Error) => {
        observer._push(error)
      },
      _push (element: QueuedResultElementOrError) {
        if (promiseHolder.resolvable !== null) {
          const resolvable = promiseHolder.resolvable
          promiseHolder.resolvable = null
          if (isError(element)) {
            resolvable.reject(element)
          } else {
            resolvable.resolve(element)
          }
        } else {
          buffer.push(element)
          onQueueSizeChanged()
        }
      },
      dequeue: dequeue,
      dequeueUntilDone: async () => {
        while (true) {
          const element = await dequeue()
          if (element.done === true) {
            return element
          }
        }
      },
      head: async () => {
        if (buffer.length > 0) {
          const element = buffer[0]
          if (isError(element)) {
            throw element
          }
          return element
        }
        promiseHolder.resolvable = createResolvablePromise()
        try {
          const element = await promiseHolder.resolvable.promise
          buffer.unshift(element)
          return element
        } catch (error) {
          buffer.unshift(error)
          throw error
        } finally {
          onQueueSizeChanged()
        }
      },
      get size (): number {
        return buffer.length
      }
    }

    return observer
  }


  private _getOrCreateCompletionPromise (): Promise<void> {
    if (this._completionPromise) {
      return this._completionPromise
    }

    this.subscribe({})

    return this._completionPromise!

  }

  async close (): Promise<void> {
    this._closed = true
    this._observer.cancel()
    await this._getOrCreateCompletionPromise()
    await this._releaseConnection()
  }
}
