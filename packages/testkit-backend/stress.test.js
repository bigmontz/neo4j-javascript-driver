
import parallelLimit from 'async/parallelLimit.js'


/*
 * 
 * STRESS TEST AREA BELLOW
 * 
 */

/**
 * Context used by the stress test
 */
class Context {
  constructor(driver, loggingEnabled, protocolVersion, bookmarks) {
    this.driver = driver
    this.bookmarks = bookmarks
    this.createdNodesCount = 0
    this._commandIdCouter = 0
    this._loggingEnabled = loggingEnabled
    this.readServersWithQueryCount = {}
    this.writeServersWithQueryCount = {}
    this.protocolVersion = protocolVersion
    this.expectedCommandsRun = 0
  }

  get commandsRun() {
    return [
      ...Object.values(this.readServersWithQueryCount),
      ...Object.values(this.writeServersWithQueryCount)
    ].reduce((a, b) => a + b, 0)
  }

  get writeCommandsRun() {
    return [...Object.values(this.writeServersWithQueryCount)].reduce(
      (a, b) => a + b,
      0
    )
  }

  queryCompleted(result, accessMode, bookmarks) {
    const serverInfo = result.summary.server
    this.protocolVersion = serverInfo.protocolVersion

    const serverAddress = serverInfo.address
    if (accessMode === 'WRITE') {
      this.createdNodesCount++
      this.writeServersWithQueryCount[serverAddress] =
        (this.writeServersWithQueryCount[serverAddress] || 0) + 1
    } else {
      this.readServersWithQueryCount[serverAddress] =
        (this.readServersWithQueryCount[serverAddress] || 0) + 1
    }

    if (bookmarks) {
      this.bookmarks = bookmarks
    }
  }

  nextCommandId() {
    return this._commandIdCouter++
  }

  readServerAddresses() {
    return Object.keys(this.readServersWithQueryCount)
  }

  writeServerAddresses() {
    return Object.keys(this.writeServersWithQueryCount)
  }

  log(commandId, message) {
    if (this._loggingEnabled) {
      console.log(`Command [${commandId}]: ${message}`)
    }
  }
}


export async function stressTest(driver) {
  await driver.executeQuery('MATCH (n) DETACH DELETE n', {}, {
    database: 'neo4j'
  })

  const TEST_MODE = {
    commandsCount: 100000,
    parallelism: 16
  }
  const READ_QUERY = 'MATCH (n) RETURN n LIMIT 1'
  const WRITE_QUERY =
    'CREATE (person:Person:Employee {name: $name, salary: $salary}) RETURN person'
  const RUNNING_TIME_IN_SECONDS  = 0


  console.time('Basic-stress-test')
  const context = new Context(
    driver,
    false,
    5.2,
    []
  )

  const printStats = () => {
    console.timeEnd('Basic-stress-test')

    console.log('Read statistics: ', context.readServersWithQueryCount)
    console.log('Write statistics: ', context.writeServersWithQueryCount)
  }

  try {
    await runWhileNotTimeout(async () => {
      const commands = createCommands(context)
      await parallelLimit(commands, TEST_MODE.parallelism)
      await verifyServers(context)
      verifyCommandsRun(context)
      await verifyNodeCount(context)
    }, RUNNING_TIME_IN_SECONDS)
  } catch (error) {
    context.error = error
  } finally {
    printStats()
    if (context.error) {
      console.error(context.error)
    }
  }

  async function runWhileNotTimeout(asyncFunc, timeoutInSeconds) {
    let shoulKeepRunning = () => true
    setTimeout(() => {
      shoulKeepRunning = () => false
    }, timeoutInSeconds * 1000)
    do {
      await asyncFunc()
    } while (shoulKeepRunning())
  }


  function createCommands(context) {
    const uniqueCommands = createUniqueCommands(context)
    function sample(arr) {
      return arr[Math.floor(Math.random() * arr.length)]
    }
    const commands = []
    for (let i = 0; i < TEST_MODE.commandsCount; i++) {
      const randomCommand = sample(uniqueCommands)
      commands.push(randomCommand)
    }

    context.expectedCommandsRun += TEST_MODE.commandsCount
    console.log(`Generated ${TEST_MODE.commandsCount} commands`)

    return commands
  }

  function createUniqueCommands(context) {
    const clusterSafeCommands = [
      readQueryInTxFunctionCommand(context),
      readQueryInTxFunctionWithBookmarksCommand(context),
      writeQueryInTxFunctionWithBookmarksCommand(context),
      writeQueryInTxFunctionCommand(context)
    ]

    return [
      ...clusterSafeCommands,
      readQueryCommand(context),
      readQueryWithBookmarksCommand(context),
      readQueryInTxCommand(context),
      readQueryInTxWithBookmarksCommand(context),
      writeQueryCommand(context),
      writeQueryWithBookmarksCommand(context),
      writeQueryInTxCommand(context),
      writeQueryInTxWithBookmarksCommand(context)
    ]
  }

  function readQueryCommand(context) {
    return queryCommand(context, READ_QUERY, () => noParams(), 'READ', false)
  }

  function readQueryWithBookmarksCommand(context) {
    return queryCommand(context, READ_QUERY, () => noParams(), 'READ', true)
  }

  function readQueryInTxCommand(context) {
    return queryInTxCommand(context, READ_QUERY, () => noParams(), 'READ', false)
  }

  function readQueryInTxFunctionCommand(context) {
    return queryInTxFunctionCommand(
      context,
      READ_QUERY,
      () => noParams(),
      'READ',
      false
    )
  }

  function readQueryInTxWithBookmarksCommand(context) {
    return queryInTxCommand(context, READ_QUERY, () => noParams(), 'READ', true)
  }

  function readQueryInTxFunctionWithBookmarksCommand(context) {
    return queryInTxFunctionCommand(
      context,
      READ_QUERY,
      () => noParams(),
      'READ',
      true
    )
  }

  function writeQueryCommand(context) {
    return queryCommand(context, WRITE_QUERY, () => randomParams(), 'WRITE', false)
  }

  function writeQueryWithBookmarksCommand(context) {
    return queryCommand(context, WRITE_QUERY, () => randomParams(), 'WRITE', true)
  }

  function writeQueryInTxCommand(context) {
    return queryInTxCommand(
      context,
      WRITE_QUERY,
      () => randomParams(),
      'WRITE',
      false
    )
  }

  function writeQueryInTxFunctionCommand(context) {
    return queryInTxFunctionCommand(
      context,
      WRITE_QUERY,
      () => randomParams(),
      'WRITE',
      false
    )
  }

  function writeQueryInTxWithBookmarksCommand(context) {
    return queryInTxCommand(
      context,
      WRITE_QUERY,
      () => randomParams(),
      'WRITE',
      true
    )
  }

  function writeQueryInTxFunctionWithBookmarksCommand(context) {
    return queryInTxFunctionCommand(
      context,
      WRITE_QUERY,
      () => randomParams(),
      'WRITE',
      true
    )
  }

  function queryCommand(
    context,
    query,
    paramsSupplier,
    accessMode,
    useBookmarks
  ) {
    return callback => {
      const commandId = context.nextCommandId()
      const session = newSession(context, accessMode, useBookmarks)
      const params = paramsSupplier()

      context.log(commandId, `About to run ${accessMode} query`)

      session
        .run(query, params)
        .then(result => {
          context.queryCompleted(result, accessMode)
          context.log(commandId, 'Query completed successfully')

          return session.close().then(() => {
            const possibleError = verifyQueryResult(result, context)
            callback(possibleError)
          })
        })
        .catch(error => {
          context.log(
            commandId,
            `Query failed with error ${JSON.stringify(error)}`
          )
          callback(error)
        })
    }
  }

  function queryInTxFunctionCommand(
    context,
    query,
    paramsSupplier,
    accessMode,
    useBookmarks
  ) {
    return callback => {
      const commandId = context.nextCommandId()
      const params = paramsSupplier()
      const session = newSession(context, accessMode, useBookmarks)

      context.log(commandId, `About to run ${accessMode} query in TX function`)

      let resultPromise
      if (accessMode === 'READ') {
        resultPromise = session.readTransaction(tx => tx.run(query, params))
      } else {
        resultPromise = session.writeTransaction(tx => tx.run(query, params))
      }

      resultPromise
        .then(result => {
          context.queryCompleted(result, accessMode, session.lastBookmarks())
          context.log(commandId, 'Transaction function executed successfully')

          return session
            .close()
            .then(() => {
              const possibleError = verifyQueryResult(result, context)
              callback(possibleError)
            })
            .catch(error => {
              context.log(
                commandId,
                `Error closing the session ${JSON.stringify(error)}`
              )
              callback(error)
            })
        })
        .catch(error => {
          context.log(
            commandId,
            `Transaction function failed with error ${JSON.stringify(error)}`
          )
          callback(error)
        })
    }
  }

  function queryInTxCommand(
    context,
    query,
    paramsSupplier,
    accessMode,
    useBookmarks
  ) {
    return callback => {
      const commandId = context.nextCommandId()
      const session = newSession(context, accessMode, useBookmarks)
      const tx = session.beginTransaction()
      const params = paramsSupplier()

      context.log(commandId, `About to run ${accessMode} query in TX`)

      tx.run(query, params)
        .then(result => {
          let commandError = verifyQueryResult(result, context)

          tx.commit()
            .catch(commitError => {
              context.log(
                commandId,
                `Transaction commit failed with error ${JSON.stringify(
                  commitError
                )}`
              )
              if (!commandError) {
                commandError = commitError
              }
            })
            .then(() => {
              context.queryCompleted(result, accessMode, session.lastBookmarks())
              context.log(commandId, 'Transaction committed successfully')

              return session.close().then(() => {
                callback(commandError)
              })
            })
        })
        .catch(error => {
          context.log(
            commandId,
            `Query failed with error ${JSON.stringify(error)}`
          )
          callback(error)
        })
    }
  }

  function verifyQueryResult(result, context) {
    if (!result) {
      return new Error('Received undefined result')
    } else if (
      result.records.length === 0 &&
      context.writeCommandsRun < TEST_MODE.parallelism
    ) {
      // it is ok to receive no nodes back for read queries at the beginning of the test
      return null
    } else if (result.records.length === 1) {
      const record = result.records[0]
      return verifyRecord(record)
    } else {
      return new Error(
        `Unexpected amount of records received: ${JSON.stringify(result)}`
      )
    }
  }

  function verifyRecord(record) {
    const node = record.get(0)

    if (!arraysEqual(['Person', 'Employee'], node.labels)) {
      return new Error(`Unexpected labels in node: ${JSON.stringify(node)}`)
    }

    const propertyKeys = Object.keys(node.properties)
    if (
      propertyKeys.length > 0 &&
      !arraysEqual(['name', 'salary'], propertyKeys)
    ) {
      return new Error(
        `Unexpected property keys in node: ${JSON.stringify(node)}`
      )
    }

    return null
  }

  function verifyCommandsRun(context) {
    if (context.commandsRun !== context.expectedCommandsRun) {
      throw new Error(
        `Unexpected commands run: ${context.commandsRun}, expected: ${context.expectedCommandsRun}`
      )
    }
  }

  function verifyNodeCount(context) {
    const expectedNodeCount = context.createdNodesCount

    const session = context.driver.session()
    return session
      .writeTransaction(tx => tx.run('MATCH (n) RETURN count(n)'))
      .then(result => {
        const record = result.records[0]
        const count = record.get(0).toNumber()

        if (count !== expectedNodeCount) {
          throw new Error(
            `Unexpected node count: ${count}, expected: ${expectedNodeCount}`
          )
        }
      })
  }

  function verifyServers(context) {
    return Promise.resolve()
  }

  function verifySingleInstance(context) {
    return new Promise(resolve => {
      const readServerAddresses = context.readServerAddresses()
      const writeServerAddresses = context.writeServerAddresses()

      if (readServerAddresses.length !== 1) {
        throw Error(
          `Expect readServerAddresses.length to be 1 but it is ${readServerAddresses.length}`
        )
      }
      if (writeServerAddresses.length !== 1) {
        throw Error(
          `Expect writeServerAddresses.length to be 1 but it is ${writeServerAddresses.length}`
        )
      }
      if (!arraysEqual(readServerAddresses, writeServerAddresses)) {
        throw Error(
          `Expect readServerAddresses (${JSON.stringify(
            readServerAddresses
          )}) to be equal to writeServerAddresses (${JSON.stringify(
            writeServerAddresses
          )}).`
        )
      }

      const address = readServerAddresses[0]
      if (context.readServersWithQueryCount[address] <= 1) {
        throw Error(
          `Expect context.readServersWithQueryCount[address] to be greater then 1, but it is ${context.readServersWithQueryCount[address]}`
        )
      }
      if (context.writeServersWithQueryCount[address] <= 1) {
        throw Error(
          `Expect context.writeServersWithQueryCount[address] to be greater then 1, but it is ${context.writeServersWithQueryCount[address]}`
        )
      }

      resolve()
    })
  }

  function randomParams() {
    return {
      name: `Person-${Date.now()}`,
      salary: Date.now()
    }
  }

  function noParams() {
    return {}
  }

  function newSession(context, accessMode, useBookmarks) {
    if (useBookmarks) {
      return context.driver.session({
        database: 'neo4j',
        defaultAccessMode: accessMode,
        bookmarks: context.bookmarks
      })
    }
    return context.driver.session({ defaultAccessMode: accessMode, database: 'neo4j' })
  }

  function arraysEqual (array1, array2) {
    const resultant = array1.filter(item => !array2.find(item2 => item2.valueOf() === item.valueOf()))
    return resultant.length === 0
  }
}
