export default {
  testEnvironment: "node",
  testMatch: ['**/dist/tests/*.test.js'],
  // Run tests serially to avoid DB conflicts
  maxConcurrency: 1,
  maxWorkers: 1,
};