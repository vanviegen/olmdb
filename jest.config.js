export default {
  testEnvironment: "node",
  testMatch: ['**/build.tests/*.test.js'],
  // Run tests serially to avoid native module conflicts
  maxConcurrency: 1,
  maxWorkers: 1,
};