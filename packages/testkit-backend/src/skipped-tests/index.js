import commonSkippedTests from './common.js'
import browserSkippedTests from './browser.js'

const skippedTestsByContext = new Map([
  ['browser', browserSkippedTests]
])

export function getShouldRunTest (contexts) {
  // const skippedTests = contexts
  //   .filter(context => skippedTestsByContext.has(context))
  //   .map(context => skippedTestsByContext.get(context))
  //   .reduce((previous, current) => [ ...previous, ...current ], commonSkippedTests)

  return (testName, { onRun, onSkip }) => {
    console.log('testName', testName)
    onRun()
    
    // const { reason } =
    //   commonSkippedTests.find(({ predicate }) => predicate(testName)) || {}
    // console.log(reason);
    // if (reason) {
    //   onSkip(reason)
    // } else {
    //   onRun()
    // }
  }
}
