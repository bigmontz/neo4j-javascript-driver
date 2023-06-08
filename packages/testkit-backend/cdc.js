
import neo4j from 'neo4j-driver-lite'

const database = 'cdctest'
const driver = neo4j.driver('neo4j://localhost:7687', neo4j.auth.none())

// await driver.executeQuery('CREATE DATABASE cdctest OPTIONS {txLogEnrichment: "FULL"}')

await driver.getServerInfo({ database })

const { records: [cdcEarliestRecord]} = await driver.executeQuery('CALL cdc.earliest()', {}, {
  database,
  routing: 'READ'
})

const from = cdcEarliestRecord.get('id')

console.log(`Reading from ${from}`)

const cdcStream = await driver.openCdcStreaming({ database, from })

process.on('SIGINT', async () => {  
  await cdcStream.close()
  await driver.close()
})

readableStream(cdcStream)

function subscription (stream) {
  stream.subscribe({
    onNext(record) {
      console.log('CDC => ', JSON.stringify(record, null, 4))
    },
    onError(error) {
      console.error(error)
    },
    onCompleted() {
      console.log('finished')
    }
  })
}

async function callback (stream) {
  await stream.consume(record => console.log('CDC => ', JSON.stringify(record, null, 4)))
  
  console.log('finished')
}

async function asyncIterator (stream) {
  for await (const record of stream) {
    console.log('CDC => ', JSON.stringify(record, null, 4))
  }
  
  console.log('Finished')
}


function readableStream (stream) {
  stream.toReadableStream()
    .on('data', cursor => console.log('cursor', cursor))
    .on('end', () => console.log('finished'))
}
