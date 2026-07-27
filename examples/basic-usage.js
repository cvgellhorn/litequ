import Queue from '../src/index.js';

async function basicExample() {
  console.log('🚀 Multi-Worker Queue Example');
  console.log('==============================\n');

  // Create a queue with custom configuration
  const queue = new Queue({
    dbPath: './example-queue.db',
    maxConcurrent: 3,
    maxRetries: 3,
    baseRetryDelay: 1000, // 1 second
    autoProcess: false, // We'll process manually for this example
  });

  // Create different job types for different operations
  const calculationJob = queue.createJob('calculation');
  const apiJob = queue.createJob('api');
  const fileJob = queue.createJob('file');

  // Set up job-specific event listeners
  calculationJob.on('completed', (info) => {
    console.log(
      `🔢 Calculation ${info.taskId} completed with result:`,
      info.result
    );
  });

  apiJob.on('completed', (info) => {
    console.log(`🌐 API call ${info.taskId} completed:`, info.result.status);
  });

  apiJob.on('retried', (info) => {
    console.log(
      `🔄 API call ${info.taskId} retry ${info.retryCount} scheduled in ${info.delay}ms`
    );
  });

  fileJob.on('completed', (info) => {
    console.log(`📁 File task ${info.taskId} completed:`, info.result);
  });

  // Set up queue-level listener to track all jobs
  queue.on('failed', (info) => {
    console.log(
      `❌ [${info.jobName}] Task ${info.taskId} permanently failed after ${info.retryCount} attempts:`,
      info.error
    );
  });

  try {
    // Add various types of tasks to their respective jobs
    console.log('Adding tasks to different job queues...\n');

    calculationJob.add({
      operation: 'multiply',
      values: [6, 7],
    });

    calculationJob.add({
      operation: 'add',
      values: [10, 20, 30],
    });

    apiJob.add({
      url: 'https://jsonplaceholder.typicode.com/posts/1',
      method: 'GET',
    });

    apiJob.add({
      url: 'https://api.example.com/data',
      method: 'POST',
      fail_chance: 0.7,
    });

    fileJob.add({
      filename: 'data.txt',
      action: 'count_lines',
    });

    fileJob.add({
      filename: 'missing.txt',
      action: 'read',
    });

    // Define job handlers
    await calculationJob.process(async (taskData) => {
      console.log(`🔄 Processing calculation: ${taskData.operation}`);
      await simulateWork(300);

      switch (taskData.operation) {
        case 'multiply':
          return taskData.values.reduce((a, b) => a * b, 1);
        case 'add':
          return taskData.values.reduce((a, b) => a + b, 0);
        default:
          throw new Error(`Unknown operation: ${taskData.operation}`);
      }
    });

    await apiJob.process(async (taskData) => {
      console.log(`🔄 Making API call to ${taskData.url}`);
      await simulateWork(800);

      // Simulate API failures
      if (taskData.fail_chance && Math.random() < taskData.fail_chance) {
        throw new Error('API call failed - network timeout');
      }

      return {
        status: 200,
        data: { title: 'Sample Post', body: 'This is a sample response' },
      };
    });

    await fileJob.process(async (taskData) => {
      console.log(`🔄 Processing file: ${taskData.filename}`);
      await simulateWork(300);

      if (taskData.filename === 'data.txt') {
        return { lines: 42, size: 1024 };
      }
      throw new Error(`File not found: ${taskData.filename}`);
    });

    // Process tasks
    console.log('\nStarting task processing...\n');

    // Process tasks multiple times to handle retries
    for (let round = 1; round <= 5; round++) {
      console.log(`--- Processing Round ${round} ---`);
      await queue._processNextBatch();

      // Wait a bit between rounds to see retry delays in action
      await new Promise((resolve) => setTimeout(resolve, 1500));

      const stats = queue.getStats();
      console.log('Queue stats by job:');
      stats.forEach((stat) => {
        console.log(`  [${stat.job_name}] ${stat.status}: ${stat.count} tasks`);
      });
      console.log('');
    }

    // Show final statistics
    console.log('Final Queue Statistics:');
    const finalStats = queue.getStats();
    finalStats.forEach((stat) => {
      console.log(`  [${stat.job_name}] ${stat.status}: ${stat.count} tasks`);
    });
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await queue.close();
    console.log('\n🏁 Queue closed');
  }
}

// Helper function to simulate work
function simulateWork(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Run the example
if (import.meta.url === `file://${process.argv[1]}`) {
  basicExample().catch(console.error);
}

export default basicExample;
