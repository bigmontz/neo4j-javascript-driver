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
import BoltProtocolV5x3 from './bolt-protocol-v5x3'

import transformersFactories from './bolt-protocol-v5x4.transformer'
import Transformer from './transformer'
import RequestMessage, { SIGNATURES } from './request-message'
import { PinMessageObserver, UnpinMessageObserver, ResultStreamObserver } from './stream-observers'

import { internal } from 'neo4j-driver-core'

const {
  constants: { BOLT_PROTOCOL_V5_4 }
} = internal

export default class BoltProtocol extends BoltProtocolV5x3 {
  get version () {
    return BOLT_PROTOCOL_V5_4
  }

  get transformer () {
    if (this._transformer === undefined) {
      this._transformer = new Transformer(Object.values(transformersFactories).map(create => create(this._config, this._log)))
    }
    return this._transformer
  }

  supportsPin () {
    return true
  }

  pinDatabase({ databaseName, impersonatedUser }, { flush, onCompleted, onError }) {
    const observer = new PinMessageObserver({ 
      onProtocolError: this._onProtocolError,
      onCompleted,
      onError
    }) 

    this.write(RequestMessage.pinDatabase({ databaseName, impersonatedUser }), observer, flush);

    return observer;
  }

  unpinDatabase({ onCompleted, onError, flush}) {
    const observer = new UnpinMessageObserver({
      onProtocolError: this._onProtocolError,
      onCompleted,
      onError
    })

    this.write(RequestMessage.unpinDatabase(), observer, flush);

    return observer;
  }

  beginTransaction ({
    bookmarks,
    txConfig,
    database,
    mode,
    impersonatedUser,
    notificationFilter,
    beforeError,
    afterError,
    beforeComplete,
    afterComplete
  } = {}) {
    const observer = new ResultStreamObserver({
      server: this._server,
      beforeError,
      afterError,
      beforeComplete,
      afterComplete
    })
    observer.prepareToHandleSingleResponse()
    let flush = true
    let sendUnpin = false

    if (this._lastMessageSignature == SIGNATURES.PIN_DATABASE) {
      flush = false
      sendUnpin = true
    }

    this.write(
      RequestMessage.begin({ bookmarks, txConfig, database, mode, impersonatedUser, notificationFilter }),
      observer,
      flush
    )

    if (sendUnpin) {
      const unpinObs = new UnpinMessageObserver({ onProtocolError: this._onProtocolError })
      this.write(RequestMessage.unpinDatabase(), unpinObs, true)
    }

    return observer
  }

  run (
    query,
    parameters,
    {
      bookmarks,
      txConfig,
      database,
      mode,
      impersonatedUser,
      notificationFilter,
      beforeKeys,
      afterKeys,
      beforeError,
      afterError,
      beforeComplete,
      afterComplete,
      flush = true,
      reactive = false,
      fetchSize = FETCH_ALL,
      highRecordWatermark = Number.MAX_VALUE,
      lowRecordWatermark = Number.MAX_VALUE
    } = {}
  ) {
    const observer = new ResultStreamObserver({
      server: this._server,
      reactive: reactive,
      fetchSize: fetchSize,
      moreFunction: this._requestMore.bind(this),
      discardFunction: this._requestDiscard.bind(this),
      beforeKeys,
      afterKeys,
      beforeError,
      afterError,
      beforeComplete,
      afterComplete,
      highRecordWatermark,
      lowRecordWatermark
    })

    const sendUnpin = this._lastMessageSignature == SIGNATURES.PIN_DATABASE  

    const flushRun = reactive
    this.write(
      RequestMessage.runWithMetadata(query, parameters, {
        bookmarks,
        txConfig,
        database,
        mode,
        impersonatedUser,
        notificationFilter
      }),
      observer,
      flushRun && flush && !sendUnpin
    )

    if (sendUnpin) {
      const unpinObs = new UnpinMessageObserver({ onProtocolError: this._onProtocolError })
      this.write(RequestMessage.unpinDatabase(), unpinObs, flushRun && flush)
    }

    if (!reactive) {
      this.write(RequestMessage.pull({ n: fetchSize }), observer, flush)
    }

    return observer
  }
}
