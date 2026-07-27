import Queue from '../src/index.js';

async function autoProcessingExample() {
  console.log('🤖 Multi-Job Auto-Processing Example');
  console.log('=====================================\n');

  // Create a queue with auto-processing enabled
  const queue = new Queue({
    dbPath: './auto-queue.db',
    maxConcurrent: 3,
    maxRetries: 2,
    baseRetryDelay: 2000, // 2 seconds
    autoProcess: true, // Enable automatic processing
  });

  // Create different jobs for different notification types
  const emailJob = queue.createJob('email');
  const smsJob = queue.createJob('sms');
  const pushJob = queue.createJob('push');

  // Job-specific event listeners
  emailJob.on('added', (info) => {
    console.log(
      `➕ [EMAIL] Task ${info.taskId} added: ${info.taskData.type} to ${info.taskData.email}`
    );
  });

  emailJob.on('completed', (info) => {
    console.log(`✅ [EMAIL] Task ${info.taskId} completed:`, info.result);
  });

  smsJob.on('added', (info) => {
    console.log(
      `➕ [SMS] Task ${info.taskId} added: ${info.taskData.type} to ${info.taskData.phone}`
    );
  });

  smsJob.on('completed', (info) => {
    console.log(`✅ [SMS] Task ${info.taskId} completed:`, info.result);
  });

  pushJob.on('added', (info) => {
    console.log(
      `➕ [PUSH] Task ${info.taskId} added: ${info.taskData.type} to ${info.taskData.deviceId}`
    );
  });

  pushJob.on('completed', (info) => {
    console.log(`✅ [PUSH] Task ${info.taskId} completed:`, info.result);
  });

  // Queue-level listeners for retries and failures
  queue.on('retried', (info) => {
    console.log(
      `🔄 [${info.jobName.toUpperCase()}] Task ${info.taskId} retry ${info.retryCount} scheduled`
    );
  });

  queue.on('failed', (info) => {
    console.log(
      `❌ [${info.jobName.toUpperCase()}] Task ${info.taskId} permanently failed:`,
      info.error
    );
  });

  queue.on('error', (info) => {
    console.error('Queue error:', info.error);
  });

  // Define handlers for each job type
  const emailHandler = async (taskData) => {
    console.log(`📧 Processing email: ${taskData.type}`);

    switch (taskData.type) {
      case 'welcome_email':
        console.log(`  Sending welcome email to ${taskData.email}`);
        await simulateWork(1000);

        // Simulate occasional email service failures
        if (Math.random() < 0.3) {
          throw new Error('Email service temporarily unavailable');
        }

        return `Welcome email sent to ${taskData.email}`;

      case 'newsletter':
        console.log(`  Sending newsletter to ${taskData.email}`);
        await simulateWork(800);

        // Simulate rate limiting
        if (Math.random() < 0.2) {
          throw new Error('Rate limit exceeded');
        }

        return `Newsletter sent to ${taskData.email}`;

      case 'password_reset':
        console.log(`  Sending password reset to ${taskData.email}`);
        await simulateWork(600);

        // This type rarely fails
        if (Math.random() < 0.1) {
          throw new Error('Invalid email address');
        }

        return `Password reset sent to ${taskData.email}`;

      default:
        throw new Error(`Unknown email type: ${taskData.type}`);
    }
  };

  const smsHandler = async (taskData) => {
    console.log(`📱 Processing SMS: ${taskData.type}`);
    await simulateWork(500);

    // SMS is generally more reliable
    if (Math.random() < 0.15) {
      throw new Error('SMS gateway temporarily unavailable');
    }

    return `SMS sent to ${taskData.phone}: ${taskData.message}`;
  };

  const pushHandler = async (taskData) => {
    console.log(`🔔 Processing push notification: ${taskData.type}`);
    await simulateWork(300);

    // Push notifications are very reliable
    if (Math.random() < 0.05) {
      throw new Error('Push service error');
    }

    return `Push sent to device ${taskData.deviceId}`;
  };

  try {
    // Start the auto-processing for all jobs
    console.log('Starting auto-processing for all jobs...\n');

    await emailJob.process(emailHandler);
    await smsJob.process(smsHandler);
    await pushJob.process(pushHandler);

    // Add some initial tasks
    console.log('Adding initial notification tasks...\n');

    emailJob.add({
      type: 'welcome_email',
      email: 'john@example.com',
      userId: 1,
    });

    smsJob.add({
      type: 'verification',
      phone: '+1234567890',
      message: 'Your verification code is 123456',
    });

    pushJob.add({
      type: 'promotion',
      deviceId: 'device-abc123',
      title: 'Special Offer!',
      body: 'Get 50% off today',
    });

    emailJob.add({
      type: 'newsletter',
      email: 'jane@example.com',
      subject: 'Weekly Updates',
    });

    // Add more tasks periodically to demonstrate wake-on-add processing
    let taskCounter = 5;
    const addTasksInterval = setInterval(async () => {
      const randomService = Math.random();

      if (randomService < 0.4) {
        // Add email task
        const emailTypes = ['welcome_email', 'newsletter', 'password_reset'];
        const randomType =
          emailTypes[Math.floor(Math.random() * emailTypes.length)];

        emailJob.add({
          type: randomType,
          email: `user${taskCounter}@test.com`,
          userId: taskCounter,
        });

        console.log(`📬 Added ${randomType} email task`);
      } else if (randomService < 0.7) {
        // Add SMS task
        smsJob.add({
          type: 'notification',
          phone: `+123456${taskCounter}`,
          message: `Notification ${taskCounter}`,
        });

        console.log(`📬 Added SMS task`);
      } else {
        // Add push task
        pushJob.add({
          type: 'update',
          deviceId: `device-${taskCounter}`,
          title: 'New Update',
          body: `Update ${taskCounter} is available`,
        });

        console.log(`📬 Added push notification task`);
      }

      taskCounter++;
    }, 3000); // Add a new task every 3 seconds

    // Show queue status periodically
    const statusInterval = setInterval(async () => {
      const stats = queue.getStats();
      const status = queue.status;

      console.log('\n📊 Queue Status:');
      console.log(
        `  Currently running: ${status.currentRunning}/${status.maxConcurrent}`
      );

      // Group stats by job
      const statsByJob = {};
      stats.forEach((stat) => {
        if (!statsByJob[stat.job_name]) {
          statsByJob[stat.job_name] = {};
        }
        statsByJob[stat.job_name][stat.status] = stat.count;
      });

      console.log('\n  Stats by job:');
      Object.entries(statsByJob).forEach(([jobName, jobStats]) => {
        const statsStr = Object.entries(jobStats)
          .map(([status, count]) => `${status}:${count}`)
          .join(', ');
        console.log(`    [${jobName}] ${statsStr}`);
      });
      console.log('');
    }, 5000);

    // Run for 30 seconds then cleanup
    setTimeout(async () => {
      console.log('\n🛑 Stopping auto-processing example...');

      clearInterval(addTasksInterval);
      clearInterval(statusInterval);

      // Process any remaining tasks
      console.log('Processing remaining tasks...');
      await new Promise((resolve) => setTimeout(resolve, 5000));

      // Show final stats
      const finalStats = queue.getStats();
      console.log('\n📈 Final Statistics by Job:');

      const statsByJob = {};
      finalStats.forEach((stat) => {
        if (!statsByJob[stat.job_name]) {
          statsByJob[stat.job_name] = {};
        }
        statsByJob[stat.job_name][stat.status] = stat.count;
      });

      Object.entries(statsByJob).forEach(([jobName, jobStats]) => {
        console.log(`\n  [${jobName.toUpperCase()}]:`);
        Object.entries(jobStats).forEach(([status, count]) => {
          console.log(`    ${status}: ${count} tasks`);
        });
      });

      await queue.close();
      console.log('\n🏁 Auto-processing example completed');
      process.exit(0);
    }, 30000); // Run for 30 seconds
  } catch (error) {
    console.error('Error in auto-processing example:', error);
    await queue.close();
    process.exit(1);
  }
}

function simulateWork(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Run the example
if (import.meta.url === `file://${process.argv[1]}`) {
  autoProcessingExample().catch(console.error);
}

export default autoProcessingExample;
