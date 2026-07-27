import Queue from '../src/index.js';

async function apiWorkerExample() {
  console.log('🌐 Multi-Service API Worker Example');
  console.log('====================================\n');

  const queue = new Queue({
    dbPath: './api-worker-queue.db',
    maxConcurrent: 4, // Process 4 API calls concurrently
    maxRetries: 3,
    baseRetryDelay: 2000,
    jitter: true, // Add randomness to retry delays
    autoProcess: true,
  });

  // Create different jobs for different API services
  const userApiJob = queue.createJob('user-api');
  const orderApiJob = queue.createJob('order-api');
  const analyticsJob = queue.createJob('analytics-api');

  // Track API call statistics
  const stats = {
    'user-api': { total: 0, successful: 0, failed: 0, retried: 0 },
    'order-api': { total: 0, successful: 0, failed: 0, retried: 0 },
    'analytics-api': { total: 0, successful: 0, failed: 0, retried: 0 },
  };

  // Job-specific event listeners
  userApiJob.on('added', (info) => {
    stats['user-api'].total++;
    console.log(
      `📤 [USER-API] Task ${info.taskId} queued: ${info.taskData.method} ${info.taskData.url}`
    );
  });

  userApiJob.on('completed', (info) => {
    stats['user-api'].successful++;
    console.log(`✅ [USER-API] Call ${info.taskId} succeeded`);
  });

  orderApiJob.on('added', (info) => {
    stats['order-api'].total++;
    console.log(
      `📤 [ORDER-API] Task ${info.taskId} queued: ${info.taskData.method} ${info.taskData.url}`
    );
  });

  orderApiJob.on('completed', (info) => {
    stats['order-api'].successful++;
    console.log(`✅ [ORDER-API] Call ${info.taskId} succeeded`);
  });

  analyticsJob.on('added', (info) => {
    stats['analytics-api'].total++;
    console.log(`📤 [ANALYTICS] Task ${info.taskId} queued`);
  });

  analyticsJob.on('completed', (info) => {
    stats['analytics-api'].successful++;
    console.log(`✅ [ANALYTICS] Call ${info.taskId} succeeded`);
  });

  // Queue-level listeners for retries and failures across all jobs
  queue.on('retried', (info) => {
    stats[info.jobName].retried++;
    console.log(
      `🔄 [${info.jobName.toUpperCase()}] Task ${info.taskId} retry ${info.retryCount} - ${info.error}`
    );
  });

  queue.on('failed', (info) => {
    stats[info.jobName].failed++;
    console.log(
      `❌ [${info.jobName.toUpperCase()}] Task ${info.taskId} permanently failed: ${info.error}`
    );
  });

  // Simulate HTTP client
  async function makeHttpRequest(method, url) {
    const delay = 200 + Math.random() * 800; // 200-1000ms response time
    await new Promise((resolve) => setTimeout(resolve, delay));

    // Simulate various HTTP responses
    const rand = Math.random();

    if (rand < 0.7) {
      // Success
      return {
        status: 200,
        data: { message: 'Success', timestamp: Date.now(), url },
        headers: { 'content-type': 'application/json' },
      };
    } else if (rand < 0.85) {
      // Server error (retryable)
      throw new Error(`HTTP 500: Internal Server Error from ${url}`);
    } else if (rand < 0.95) {
      // Network timeout (retryable)
      throw new Error(`Network timeout for ${url}`);
    } else {
      // Client error (not retryable, but we'll retry anyway for demo)
      throw new Error(`HTTP 404: Not Found - ${url}`);
    }
  }

  // Define handlers for each API service
  const userApiHandler = async (taskData) => {
    const { method, url, headers, body } = taskData;
    console.log(`🔄 [USER-API] Making ${method} request to ${url}`);

    try {
      const response = await makeHttpRequest(method, url, {
        headers,
        body,
      });

      return {
        status: response.status,
        success: true,
        url: url,
        responseTime: `${Math.floor(Math.random() * 800 + 200)}ms`,
      };
    } catch (error) {
      throw new Error(`User API call failed: ${error.message}`);
    }
  };

  const orderApiHandler = async (taskData) => {
    const { method, url, headers, body } = taskData;
    console.log(`🔄 [ORDER-API] Making ${method} request to ${url}`);

    try {
      const response = await makeHttpRequest(method, url, {
        headers,
        body,
      });

      return {
        status: response.status,
        success: true,
        url: url,
        responseTime: `${Math.floor(Math.random() * 800 + 200)}ms`,
      };
    } catch (error) {
      throw new Error(`Order API call failed: ${error.message}`);
    }
  };

  const analyticsHandler = async (taskData) => {
    const { event } = taskData;
    console.log(`🔄 [ANALYTICS] Tracking event: ${event}`);

    try {
      await simulateWork(300);
      // Simulate analytics calls being more reliable
      if (Math.random() < 0.9) {
        return { tracked: true, event, timestamp: Date.now() };
      }
      throw new Error('Analytics service timeout');
    } catch (error) {
      throw new Error(`Analytics call failed: ${error.message}`);
    }
  };

  async function simulateWork(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  try {
    console.log('Starting multi-service API worker...\n');

    // Register handlers for each job
    await userApiJob.process(userApiHandler);
    await orderApiJob.process(orderApiHandler);
    await analyticsJob.process(analyticsHandler);

    // Add various API tasks to different services
    console.log('Adding API tasks to different services...\n');

    // User API tasks
    userApiJob.add({
      method: 'GET',
      url: 'https://api.example.com/users',
      headers: { Authorization: 'Bearer token123' },
    });

    userApiJob.add({
      method: 'POST',
      url: 'https://api.example.com/users',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'John Doe', email: 'john@example.com' }),
    });

    // Order API tasks
    orderApiJob.add({
      method: 'GET',
      url: 'https://api.example.com/orders/12345',
    });

    orderApiJob.add({
      method: 'PUT',
      url: 'https://api.example.com/orders/12345/status',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status: 'shipped' }),
    });

    // Analytics tasks
    analyticsJob.add({
      event: 'page_view',
      properties: { page: '/home', user_id: 123 },
    });

    analyticsJob.add({
      event: 'purchase',
      properties: { order_id: 12345, amount: 99.99 },
    });

    // Add more tasks periodically
    let taskId = 7;
    const addMoreTasks = setInterval(async () => {
      const randomService = Math.random();

      if (randomService < 0.4) {
        // User API
        userApiJob.add({
          method: 'GET',
          url: `https://api.example.com/users/${taskId}`,
          headers: { 'X-Request-ID': `req-${taskId}` },
        });
      } else if (randomService < 0.7) {
        // Order API
        orderApiJob.add({
          method: 'GET',
          url: `https://api.example.com/orders/${taskId}`,
          headers: { 'X-Request-ID': `req-${taskId}` },
        });
      } else {
        // Analytics
        analyticsJob.add({
          event: 'custom_event',
          properties: { event_id: taskId },
        });
      }

      taskId++;
    }, 2000);

    // Show statistics periodically
    const showStats = setInterval(() => {
      console.log('\n📊 API Worker Statistics:');

      Object.entries(stats).forEach(([jobName, jobStats]) => {
        const total = jobStats.total;
        const successRate =
          total > 0 ? ((jobStats.successful / total) * 100).toFixed(1) : 0;

        console.log(`\n  [${jobName.toUpperCase()}]:`);
        console.log(`    Total: ${jobStats.total}`);
        console.log(`    Successful: ${jobStats.successful}`);
        console.log(`    Failed: ${jobStats.failed}`);
        console.log(`    Retried: ${jobStats.retried}`);
        console.log(`    Success rate: ${successRate}%`);
      });

      const queueStatus = queue.status;
      console.log(
        `\n  Currently processing: ${queueStatus.currentRunning}/${queueStatus.maxConcurrent}`
      );
      console.log('');
    }, 5000);

    // Run for 25 seconds
    setTimeout(async () => {
      console.log('\n🛑 Stopping API worker...');

      clearInterval(addMoreTasks);
      clearInterval(showStats);

      // Wait for remaining tasks
      console.log('Waiting for remaining tasks to complete...');
      await new Promise((resolve) => setTimeout(resolve, 5000));

      // Final statistics
      console.log('\n📈 Final API Worker Statistics:');

      Object.entries(stats).forEach(([jobName, jobStats]) => {
        const total = jobStats.total;
        const successRate =
          total > 0 ? ((jobStats.successful / total) * 100).toFixed(1) : 0;
        const failureRate =
          total > 0 ? ((jobStats.failed / total) * 100).toFixed(1) : 0;

        console.log(`\n  [${jobName.toUpperCase()}]:`);
        console.log(`    Total API calls: ${total}`);
        console.log(`    Successful: ${jobStats.successful} (${successRate}%)`);
        console.log(`    Failed: ${jobStats.failed} (${failureRate}%)`);
        console.log(`    Total retries: ${jobStats.retried}`);
      });

      const dbStats = queue.getStats();
      console.log('\n📋 Database Statistics (by job and status):');
      dbStats.forEach((stat) => {
        console.log(`  [${stat.job_name}] ${stat.status}: ${stat.count} tasks`);
      });

      await queue.close();
      console.log('\n🏁 API worker example completed');
      process.exit(0);
    }, 25000);
  } catch (error) {
    console.error('Error in API worker example:', error);
    await queue.close();
    process.exit(1);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  apiWorkerExample().catch(console.error);
}

export default apiWorkerExample;
