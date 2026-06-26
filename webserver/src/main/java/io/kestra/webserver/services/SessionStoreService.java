package io.kestra.webserver.services;

import java.sql.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Optional;
import java.util.Set;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micronaut.context.annotation.Property;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Service for managing SSO session storage and validation.
 * 
 * Redis-only architecture:
 * - Primary storage: Redis (fast, distributed)
 * - Fallback storage: In-memory ConcurrentHashMap (development only)
 * 
 * Sessions are stored with keys following the pattern: sid_{timestamp}_{random}
 * 
 * Thread-safe: All operations are thread-safe for concurrent access.
 */
@Slf4j
@Singleton
public class SessionStoreService {
    private static final Logger LOG = LoggerFactory.getLogger(SessionStoreService.class);

    /**
     * Session ID pattern for validation.
     * Format: sid_{timestamp}_{randomString}
     * Example: sid_1780655429477_el9zmm5
     */
    private static final String SESSION_ID_PATTERN = "^sid_\\d+_[a-z0-9]+$";

    /**
     * In-memory fallback storage for development/local mode.
     */
    private static final ConcurrentHashMap<String, SessionData> inMemorySessions = new ConcurrentHashMap<>();

    /**
     * Redis connection pool (optional) - created lazily if redis.enabled=true
     */
    private Optional<JedisPool> redisPool;

    /**
     * JSON object mapper for session serialization.
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Redis configuration properties - bound from application.yml redis.* prefix
     */
    private boolean redisEnabled = false;
    private String redisUrl = "redis://localhost:6379";
    private int redisConnectionPoolSize = 10;
    private int redisTimeoutMs = 3000;
    private int redisDatabase = 0;

    public SessionStoreService(
        @Property(name = "redis.enabled", defaultValue = "false") boolean redisEnabled,
        @Property(name = "redis.url", defaultValue = "redis://localhost:6379") String redisUrl,
        @Property(name = "redis.connection-pool-size", defaultValue = "10") int redisConnectionPoolSize,
        @Property(name = "redis.timeout-ms", defaultValue = "3000") int redisTimeoutMs,
        @Property(name = "redis.database", defaultValue = "0") int redisDatabase
    ) {
        this.redisEnabled = redisEnabled;
        this.redisUrl = redisUrl;
        this.redisConnectionPoolSize = redisConnectionPoolSize;
        this.redisTimeoutMs = redisTimeoutMs;
        this.redisDatabase = redisDatabase;
        this.redisPool = Optional.empty(); // Will be initialized in @PostConstruct
        
        LOG.info("[KC-STORE] SessionStoreService constructor: redisEnabled={}, redisUrl={}",
            redisEnabled, redisUrl);
    }

    @PostConstruct
    public void init() {
        LOG.info("[KC-STORE] @PostConstruct init: redisEnabled={}", redisEnabled);
        
        // Initialize Redis pool if enabled
        if (redisEnabled) {
            this.redisPool = createRedisPool();
        } else {
            LOG.warn("[KC-STORE] Redis disabled - using in-memory sessions only");
        }
    }

    private Optional<JedisPool> createRedisPool() {
        try {
            LOG.info("[KC-STORE] Creating JedisPool from: {}", redisUrl);
            java.net.URI redisUri = new java.net.URI(redisUrl);
            String host = redisUri.getHost() != null ? redisUri.getHost() : "localhost";
            int port = redisUri.getPort() > 0 ? redisUri.getPort() : 6379;
            
            String password = null;
            if (redisUri.getUserInfo() != null) {
                String[] userInfo = redisUri.getUserInfo().split(":");
                if (userInfo.length == 2) {
                    password = userInfo[1];
                }
            }

            redis.clients.jedis.JedisPoolConfig poolConfig = new redis.clients.jedis.JedisPoolConfig();
            poolConfig.setMaxTotal(redisConnectionPoolSize);
            poolConfig.setMaxIdle(redisConnectionPoolSize / 2);
            poolConfig.setMinIdle(2);
            poolConfig.setTestOnBorrow(true);
            poolConfig.setTestOnReturn(true);
            poolConfig.setTestWhileIdle(true);
            poolConfig.setMinEvictableIdleTimeMillis(60000);
            poolConfig.setTimeBetweenEvictionRunsMillis(30000);
            poolConfig.setNumTestsPerEvictionRun(3);
            poolConfig.setBlockWhenExhausted(true);

            JedisPool pool = new JedisPool(poolConfig, host, port, redisTimeoutMs, password, redisDatabase);

            try (Jedis jedis = pool.getResource()) {
                String pong = jedis.ping();
                LOG.info("[KC-STORE] ✓ Connected to Redis at {}:{} - Response: {}", host, port, pong);
            }

            return Optional.of(pool);

        } catch (Exception e) {
            LOG.error("[KC-STORE] ✗ Failed to initialize Redis: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Validates if a session exists and is not expired.
     * Redis-only mode: no PostgreSQL fallback.
     * 
     * @param sessionId The session ID to validate (pattern: sid_xxx)
     * @return true if session exists and is not expired, false otherwise
     */
    public boolean isSessionValid(String sessionId) {
        long startTime = System.nanoTime();
        
        if (sessionId == null || !sessionId.matches(SESSION_ID_PATTERN)) {
            LOG.debug("[KC-STORE] isSessionValid: sessionId='{}' INVALID (null or bad pattern)", sessionId);
            return false;
        }

        // Try Redis first if available
        if (redisPool.isPresent()) {
            try {
                long redisStartTime = System.nanoTime();
                Boolean result = isSessionValidRedis(sessionId);
                long redisElapsedMs = (System.nanoTime() - redisStartTime) / 1_000_000;
                long totalElapsedMs = (System.nanoTime() - startTime) / 1_000_000;
                
                if (result != null) {
                    return result;
                }
                
                return false;
                
            } catch (Exception e) {
                long redisElapsedMs = (System.nanoTime() - startTime) / 1_000_000;
                LOG.error("[KC-STORE] Redis error after {}ms: {}", redisElapsedMs, e.getMessage());
                return false;
            }
        }

        // Fallback to in-memory (development only)
        LOG.warn("[KC-STORE] Redis not available, falling back to in-memory");
        long memStartTime = System.nanoTime();
        boolean result = isSessionValidInMemory(sessionId);
        long memElapsedMs = (System.nanoTime() - memStartTime) / 1_000_000;
        long totalElapsedMs = (System.nanoTime() - startTime) / 1_000_000;
        
        LOG.info("[KC-STORE-TIMING] Session validation (in-memory fallback): sessionId='{}' result={} inMemoryLookup={}ms total={}ms", 
            sessionId, result, memElapsedMs, totalElapsedMs);
        return result;
    }

    private Boolean isSessionValidRedis(String sessionId) {
        if (!redisPool.isPresent()) {
            return null;
        }

        try (Jedis jedis = redisPool.get().getResource()) {
            String key = "session:" + sessionId;
            
            // Get HASH data
            java.util.Map<String, String> hashData = jedis.hgetAll(key);
            
            if (hashData == null || hashData.isEmpty()) {
                LOG.debug("[KC-STORE] isSessionValid (Redis): sessionId='{}' NOT FOUND", sessionId);
                return false;
            }

            // Parse session to check expiration
            SessionData session = hashToSessionData(hashData);
            long now = System.currentTimeMillis();
            
            if (session.getSessionExpiresAt() < now) {
                // Session expired, remove from Redis
                jedis.del(key);
                LOG.warn("[KC-STORE] isSessionValid (Redis): sessionId='{}' EXPIRED", sessionId);
                return false;
            }

            LOG.debug(
                "[KC-STORE] isSessionValid (Redis): sessionId='{}' OK (expiresIn={}s)",
                sessionId, (session.getSessionExpiresAt() - now) / 1000
            );
            return true;

        } catch (Exception e) {
            LOG.warn("[KC-STORE] isSessionValid (Redis): Error checking session: {}", e.getMessage());
            return null;
        }
    }

    private boolean isSessionValidInMemory(String sessionId) {
        SessionData session = inMemorySessions.get(sessionId);
        if (session == null) {
            LOG.debug(
                "[KC-STORE] isSessionValid (in-memory): sessionId='{}' NOT FOUND (size={})",
                sessionId, inMemorySessions.size()
            );
            return false;
        }

        long now = System.currentTimeMillis();
        if (session.getSessionExpiresAt() < now) {
            inMemorySessions.remove(sessionId);
            LOG.warn("[KC-STORE] isSessionValid (in-memory): sessionId='{}' EXPIRED", sessionId);
            return false;
        }

        LOG.debug(
            "[KC-STORE] isSessionValid (in-memory): sessionId='{}' OK (expiresIn={}s)",
            sessionId, (session.getSessionExpiresAt() - now) / 1000
        );
        return true;
    }

    /**
     * Retrieves session data for the given session ID.
     * 
     * @param sessionId The session ID (pattern: sid_xxx)
     * @return SessionData if session is valid, null otherwise
     */
    public SessionData getSession(String sessionId) {

        // Try Redis first if available
        if (redisPool.isPresent()) {
            try {
                SessionData result = getSessionRedis(sessionId);
                if (result != null) {
                    return result;
                }
            } catch (Exception e) {
                LOG.warn("[KC-STORE] getSession: Redis error, falling back: {}", e.getMessage());
            }
        }

        // Fallback to in-memory
        return getSessionInMemory(sessionId);
    }

    private SessionData getSessionRedis(String sessionId) {
        if (!redisPool.isPresent()) {
            return null;
        }

        try (Jedis jedis = redisPool.get().getResource()) {
            String key = "session:" + sessionId;
            
            // Get HASH data
            java.util.Map<String, String> hashData = jedis.hgetAll(key);
            
            if (hashData == null || hashData.isEmpty()) {
                LOG.debug("[KC-STORE] getSession (Redis): sessionId='{}' NOT FOUND", sessionId);
                return null;
            }

            SessionData session = hashToSessionData(hashData);
            LOG.debug("[KC-STORE] getSession (Redis): sessionId='{}' FOUND", sessionId);
            return session;

        } catch (Exception e) {
            LOG.warn("[KC-STORE] getSession (Redis): Error retrieving session: {}", e.getMessage());
            return null;
        }
    }

    private SessionData getSessionInMemory(String sessionId) {
        SessionData session = inMemorySessions.get(sessionId);
        if (session == null) {
            LOG.debug("[KC-STORE] getSession (in-memory): sessionId='{}' NOT FOUND", sessionId);
            return null;
        }
        return session;
    }

    /**
     * Stores a new session or updates an existing one.
     * 
     * @param sessionId The session ID (pattern: sid_xxx)
     * @param data The session data to store
     */
    public void storeSession(String sessionId, SessionData data) {
        if (sessionId == null || data == null) {
            throw new IllegalArgumentException("Session ID and data must not be null");
        }

        // Store to Redis if available
        if (redisPool.isPresent()) {
            try {
                storeSessionRedis(sessionId, data);
            } catch (Exception e) {
                LOG.warn("[KC-STORE] storeSession: Redis error, continuing: {}", e.getMessage());
            }
        }

        // Store to in-memory as backup
        storeSessionInMemory(sessionId, data);
    }

    private void storeSessionRedis(String sessionId, SessionData data) {
        if (!redisPool.isPresent()) {
            return;
        }

        try (Jedis jedis = redisPool.get().getResource()) {
            String key = "session:" + sessionId;
            
            // Convert SessionData to HASH map
            java.util.Map<String, String> hashData = sessionDataToHash(data);
            
            // Store as HASH
            jedis.hset(key, hashData);
            
            // Set TTL based on session expiration
            long ttl = (data.getSessionExpiresAt() - System.currentTimeMillis()) / 1000;
            if (ttl > 0) {
                jedis.expire(key, (int) ttl);
            }

            LOG.info(
                "[KC-STORE] storeSession (Redis): sessionId='{}' ttl={}s email={} type=HASH",
                sessionId, ttl, data.getEmail()
            );

        } catch (Exception e) {
            LOG.warn("[KC-STORE] storeSession (Redis): Error storing session: {}", e.getMessage());
        }
    }

    private void storeSessionInMemory(String sessionId, SessionData data) {
        boolean replaced = inMemorySessions.containsKey(sessionId);
        inMemorySessions.put(sessionId, data);
        LOG.info(
            "[KC-STORE] storeSession (in-memory): sessionId='{}' action={} expiresAt={} size={}",
            sessionId, replaced ? "UPDATED" : "CREATED", data.getExpiresAt(), inMemorySessions.size()
        );
    }

    /**
     * Removes a session from the store.
     * 
     * @param sessionId The session ID to remove
     */
    public void removeSession(String sessionId) {
        if (sessionId == null) {
            throw new IllegalArgumentException("Session ID must not be null");
        }

        // Remove from Redis
        if (redisPool.isPresent()) {
            removeSessionRedis(sessionId);
        }

        // Remove from in-memory
        removeSessionInMemory(sessionId);
    }

    private void removeSessionRedis(String sessionId) {
        if (!redisPool.isPresent()) {
            return;
        }

        try (Jedis jedis = redisPool.get().getResource()) {
            String key = "session:" + sessionId;
            jedis.del(key);
            LOG.info("[KC-STORE] removeSession (Redis): sessionId='{}' deleted", sessionId);
        } catch (Exception e) {
            LOG.warn("[KC-STORE] removeSession (Redis): Error: {}", e.getMessage());
        }
    }

    private void removeSessionInMemory(String sessionId) {
        inMemorySessions.remove(sessionId);
        LOG.info("[KC-STORE] removeSession (in-memory): sessionId='{}' deleted", sessionId);
    }

    /**
     * Clears all sessions from the store.
     * WARNING: This is a destructive operation.
     */
    public void clearAllSessions() {
        if (redisPool.isPresent()) {
            try (Jedis jedis = redisPool.get().getResource()) {
                jedis.flushDB();
                LOG.info("[KC-STORE] clearAllSessions (Redis): cleared");
            } catch (Exception e) {
                LOG.error("[KC-STORE] clearAllSessions (Redis): Error: {}", e.getMessage());
            }
        }
        
        inMemorySessions.clear();
        LOG.info("[KC-STORE] clearAllSessions (in-memory): cleared");
    }

    /**
     * Gets the current number of sessions stored.
     */
    public int getSessionCount() {
        if (redisPool.isPresent()) {
            try (Jedis jedis = redisPool.get().getResource()) {
                return (int) jedis.dbSize();
            } catch (Exception e) {
                LOG.warn("[KC-STORE] getSessionCount (Redis): Error: {}", e.getMessage());
            }
        }
        return inMemorySessions.size();
    }

    /**
     * Alias for getSessionCount() for backward compatibility.
     */
    public int size() {
        return getSessionCount();
    }

    /**
     * Gets the refresh_token from session.
     */
    public String getRefreshToken(String sessionId) {
        SessionData session = getSession(sessionId);
        if (session == null) {
            return null;
        }
        return session.getRefreshToken();
    }

    /**
     * Checks if access token is expired or about to expire.
     */
    public boolean isAccessTokenExpired(String sessionId) {
        SessionData session = getSession(sessionId);
        if (session == null) {
            LOG.debug("[KC-STORE] isAccessTokenExpired: session not found for sessionId={}", sessionId);
            return true;
        }

        long now = System.currentTimeMillis();
        long accessTokenExpiresAt = session.getAccessTokenExpiresAt();

        if (accessTokenExpiresAt <= 0) {
            LOG.debug("[KC-STORE] isAccessTokenExpired: accessTokenExpiresAt not set, treating as expired for sessionId={}", sessionId);
            return true;
        }

        boolean isExpired = accessTokenExpiresAt < (now + 30_000L);
        if (isExpired) {
            LOG.info(
                "[KC-STORE] isAccessTokenExpired: token expires soon ({}ms) for sessionId={}",
                (accessTokenExpiresAt - now), sessionId
            );
        }
        return isExpired;
    }

    /**
     * Converts SessionData object to HASH map for Redis storage.
     */
    private java.util.Map<String, String> sessionDataToHash(SessionData data) {
        java.util.Map<String, String> hash = new java.util.HashMap<>();
        
        if (data.getEmail() != null) hash.put("email", data.getEmail());
        if (data.getAccessToken() != null) hash.put("access_token", data.getAccessToken());
        if (data.getRefreshToken() != null) hash.put("refresh_token", data.getRefreshToken());
        if (data.getIdToken() != null) hash.put("id_token", data.getIdToken());
        if (data.getUserSub() != null) hash.put("user_sub", data.getUserSub());
        
        hash.put("created_at", String.valueOf(data.getCreatedAt()));
        hash.put("access_token_expires_at", String.valueOf(data.getAccessTokenExpiresAt()));
        hash.put("session_expires_at", String.valueOf(data.getSessionExpiresAt()));
        hash.put("expires_at", String.valueOf(data.getExpiresAt()));
        
        return hash;
    }

    /**
     * Converts HASH map from Redis to SessionData object.
     */
    private SessionData hashToSessionData(java.util.Map<String, String> hash) {
        SessionData data = new SessionData();
        
        if (hash.containsKey("email")) data.setEmail(hash.get("email"));
        if (hash.containsKey("access_token")) data.setAccessToken(hash.get("access_token"));
        if (hash.containsKey("refresh_token")) data.setRefreshToken(hash.get("refresh_token"));
        if (hash.containsKey("id_token")) data.setIdToken(hash.get("id_token"));
        if (hash.containsKey("user_sub")) data.setUserSub(hash.get("user_sub"));
        
        try {
            if (hash.containsKey("created_at")) {
                data.setCreatedAt(Long.parseLong(hash.get("created_at")));
            }
            if (hash.containsKey("access_token_expires_at")) {
                data.setAccessTokenExpiresAt(Long.parseLong(hash.get("access_token_expires_at")));
            }
            if (hash.containsKey("session_expires_at")) {
                data.setSessionExpiresAt(Long.parseLong(hash.get("session_expires_at")));
            }
            if (hash.containsKey("expires_at")) {
                data.setExpiresAt(Long.parseLong(hash.get("expires_at")));
            }
        } catch (NumberFormatException e) {
            LOG.warn("[KC-STORE] hashToSessionData: Error parsing timestamp: {}", e.getMessage());
        }
        
        return data;
    }

    /**
     * Validates a Bearer token (email or access_token format) and extracts session data.
     * 
     * Token format detection (simple, no validation):
     * 1. Short token (length < 100) → Treat as email (e.g., "user@example.com")
     * 2. Long token (length >= 100) → Treat as access_token/JWT (e.g., "eyJhbGc...")
     * 
     * @param token The Bearer token value
     * @return SessionData with user info if valid
     * @throws IllegalArgumentException if token is empty
     * @throws IllegalStateException if token parsing fails
     */
    /**
     * ⚠️ DEPRECATED: Use findBearerTokenInRedis() instead for proper validation.
     * This method is kept for backward compatibility but has a security weakness:
     * It does NOT validate the token against Redis, accepting any email-format token.
     * 
     * @param token The bearer token (email or JWT)
     * @return SessionData with extracted information
     * @throws IllegalArgumentException if token is empty
     * @throws IllegalStateException if JWT is expired
     */
    public SessionData validateBearerToken(String token) {
        if (token == null || token.isBlank()) {
            LOG.warn("[KC-STORE] validateBearerToken: Token cannot be empty");
            throw new IllegalArgumentException("Token cannot be empty");
        }

        // Simple length-based detection: < 100 chars = email, >= 100 chars = access_token/JWT
        if (token.length() < 100) {
            // Treat as email format
            LOG.debug("[KC-STORE] validateBearerToken: Treating short token as email (length={}): {}", token.length(), token);
            SessionData sessionData = new SessionData();
            sessionData.setEmail(token);
            // Set expiration to 1 hour from now
            sessionData.setAccessTokenExpiresAt(System.currentTimeMillis() + 3600000);
            return sessionData;
        }

        // Treat as access_token/JWT format: try to parse as JWT
        try {
            String[] parts = token.split("\\.");
            if (parts.length != 3) {
                // Not a valid JWT structure - but still accept it as a valid access token
                LOG.debug("[KC-STORE] validateBearerToken: Long token without JWT structure, accepting as access_token (length={})", token.length());
                SessionData sessionData = new SessionData();
                // Extract email from token if it contains @ (fallback)
                if (token.contains("@")) {
                    int atIndex = token.indexOf('@');
                    String potentialEmail = token.substring(0, atIndex + token.length() - atIndex);
                    if (potentialEmail.contains("@")) {
                        sessionData.setEmail(potentialEmail);
                    }
                }
                sessionData.setAccessTokenExpiresAt(System.currentTimeMillis() + 3600000);
                return sessionData;
            }

            // Decode JWT payload (second part)
            String payload = new String(java.util.Base64.getUrlDecoder().decode(parts[1]));
            SessionData sessionData = OBJECT_MAPPER.readValue(payload, SessionData.class);

            // Validate expiration if present
            if (sessionData.getAccessTokenExpiresAt() > 0) {
                long now = System.currentTimeMillis();
                if (sessionData.getAccessTokenExpiresAt() < now) {
                    long expiredSince = now - sessionData.getAccessTokenExpiresAt();
                    LOG.warn("[KC-STORE] validateBearerToken: JWT expired {} ms ago", expiredSince);
                    throw new IllegalStateException("Token has expired");
                }
            }

            LOG.debug("[KC-STORE] validateBearerToken: Valid JWT for email={}", sessionData.getEmail());
            return sessionData;

        } catch (IllegalStateException e) {
            // Re-throw our validation errors
            throw e;
        } catch (Exception e) {
            // If JWT parsing fails, still accept the token as valid (lenient mode)
            LOG.debug("[KC-STORE] validateBearerToken: JWT parsing failed, but accepting as valid access_token: {}", e.getMessage());
            SessionData sessionData = new SessionData();
            sessionData.setAccessTokenExpiresAt(System.currentTimeMillis() + 3600000);
            return sessionData;
        }
    }

    /**
     * ✨ NEW: Validates Bearer token by checking if it exists as a valid access token in Redis.
     * 
     * For email-format tokens (< 100 chars), we don't have a way to validate them in Redis,
     * so we only validate them against active sessions.
     * 
     * For JWT tokens (>= 100 chars), we:
     * 1. Parse the JWT
     * 2. Search Redis for any session that contains this token as accessToken
     * 3. Verify the token hasn't expired
     * 
     * @param token The bearer token (email or JWT access_token)
     * @return SessionData if token is valid and found in Redis
     * @throws IllegalArgumentException if token is empty or invalid format
     * @throws IllegalStateException if token not found in Redis or has expired
     */
    public SessionData findBearerTokenInRedis(String token) {
        if (token == null || token.isBlank()) {
            LOG.warn("[KC-STORE] findBearerTokenInRedis: Token cannot be empty");
            throw new IllegalArgumentException("Token cannot be empty");
        }

        // If token is short (< 100 chars), treat as email - no Redis lookup possible
        if (token.length() < 100) {
            LOG.debug("[KC-STORE] findBearerTokenInRedis: Short token (email format), cannot validate in Redis: {}", token);
            // For email tokens, we accept them as-is (they're used as Bearer value directly)
            // In a real scenario, you might want to verify this email exists in a user database
            throw new IllegalStateException("Email-format Bearer tokens cannot be validated - use full access tokens");
        }

        // Long token (>= 100 chars) - treat as access token/JWT, search Redis
        if (!redisPool.isPresent()) {
            LOG.warn("[KC-STORE] findBearerTokenInRedis: Redis not available");
            throw new IllegalStateException("Redis not available for token validation");
        }

        try (Jedis jedis = redisPool.get().getResource()) {
            // Search all session keys for a matching accessToken
            Set<String> sessionKeys = jedis.keys("session:sid_*");
            
            if (sessionKeys.isEmpty()) {
                LOG.debug("[KC-STORE] findBearerTokenInRedis: No sessions found in Redis");
                throw new IllegalStateException("Token not found - no active sessions");
            }

            for (String sessionKey : sessionKeys) {
                try {
                    // Get HASH data
                    java.util.Map<String, String> hashData = jedis.hgetAll(sessionKey);
                    if (hashData == null || hashData.isEmpty()) continue;
                    
                    SessionData session = hashToSessionData(hashData);
                    
                    // Check if this session's accessToken matches the Bearer token
                    if (session.getAccessToken() != null && session.getAccessToken().equals(token)) {
                        // Check if token is expired
                        long now = System.currentTimeMillis();
                        if (session.getAccessTokenExpiresAt() > 0 && session.getAccessTokenExpiresAt() < now) {
                            LOG.warn("[KC-STORE] findBearerTokenInRedis: Token expired {} ms ago for email={}", 
                                (now - session.getAccessTokenExpiresAt()), session.getEmail());
                            throw new IllegalStateException("Token has expired");
                        }
                        
                        LOG.info("[KC-STORE] findBearerTokenInRedis: ✅ Valid token found for email={}", session.getEmail());
                        return session;
                    }
                } catch (IllegalStateException e) {
                    throw e;  // Re-throw our validation errors
                } catch (Exception e) {
                    LOG.debug("[KC-STORE] findBearerTokenInRedis: Error parsing session {}: {}", sessionKey, e.getMessage());
                    // Continue to next session
                }
            }

            // Token not found in any session
            LOG.warn("[KC-STORE] findBearerTokenInRedis: ❌ Token not found in Redis (searched {} sessions)", sessionKeys.size());
            throw new IllegalStateException("Bearer token not found in Redis - invalid or revoked");

        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("[KC-STORE] findBearerTokenInRedis: Redis error: {}", e.getMessage());
            throw new IllegalStateException("Error validating token: " + e.getMessage());
        }
    }
}
