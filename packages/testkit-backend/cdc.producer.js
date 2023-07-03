import neo4j from 'neo4j-driver-lite'

const database = 'cdctest'

const driver = neo4j.driver('neo4j://localhost:7687', neo4j.auth.none())


let id = 0

const intervalId  = setInterval(async () => {
  console.log('creating user')
  await driver.executeQuery('CREATE (:Person{ name: $name, age: $age})',
    { 
      name: `User ${id}`, 
      age: neo4j.int(id++)
    },
    {
      database
    })
    
  console.log('user created')
}, 20)




process.on('SIGINT', async () => {  
  clearInterval(intervalId)
  await driver.close()
})

