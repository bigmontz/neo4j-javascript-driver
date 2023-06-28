
import neo4j from 'neo4j-driver-lite'

const database = 'cdctest'
const driver = neo4j.driver('neo4j://localhost:7687', neo4j.auth.none(), {
  logging: neo4j.logging.console('debug')
})

//await driver.executeQuery('CREATE DATABASE cdctest OPTIONS {txLogEnrichment: "FULL"}')

await driver.getServerInfo({ database })

let [, , from] = process.argv

if (from == null) {
  
  const { records: [cdcEarliestRecord]} = await driver.executeQuery('CALL cdc.earliest()', {}, {
    database,
    routing: 'READ'
  })

  from = cdcEarliestRecord.get('id')
}

console.log(`Reading from ${from}`)

const cdcStream = await driver.openCdcStreaming({ database, from })

process.on('SIGINT', async () => {  
  await cdcStream.close()
  await driver.close()
})

await asyncIterator(cdcStream)

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
    //console.log('CDC => ', JSON.stringify(record, null, 4))
    console.log(`[${Date.now()}] Reading changes ${record._fields[0]} => current id ${stream._currentChangeIdentifier}`)
  }
  
  console.log('Finished')
}


function readableStream (stream) {
  stream.toReadableStream()
    .on('data', cursor => console.log('cursor', cursor))
    .on('end', () => console.log('finished'))
}
