package io.kestra.webserver.services;

import io.micronaut.context.annotation.ConfigurationInject;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.net.URI;
import java.util.Optional;

/**
 * Redis pool factory for SessionStoreService.
 * Creates JedisPool bean if Redis is configured and enabled.
 */
@Slf4j
@Factory
public class RedisPoolFactory {

    @Getter
    @NoArgsConstructor
    @ConfigurationProperties("redis")
    public static class RedisProperties {
        private boolean enabled = false;
        private String url = "redis://localhost:6379";
        private int connectionPoolSize = 10;
        private int timeoutMs = 3000;
        private int database = 0;
        private boolean ssl = false;

        @ConfigurationInject
        public RedisProperties(
            Boolean enabled,
            String url,
            Integer connectionPoolSize,
            Integer timeoutMs,
            Integer database,
            Boolean ssl
        ) {
            System.out.println("[REDIS-PROPS-CTOR] RedisProperties constructor:");
            System.out.println("[REDIS-PROPS-CTOR]   enabled=" + enabled);
            System.out.println("[REDIS-PROPS-CTOR]   url=" + url);
            
            this.enabled = enabled != null && enabled;
            this.url = url != null ? url.trim() : "redis://localhost:6379";
            this.connectionPoolSize = connectionPoolSize != null ? connectionPoolSize : 10;
            this.timeoutMs = timeoutMs != null ? timeoutMs : 3000;
            this.database = database != null ? database : 0;
            this.ssl = ssl != null && ssl;
            
            System.out.println("[REDIS-PROPS-CTOR]   this.enabled=" + this.enabled + " (after assignment)");
        }
    }

    @Singleton
    public Optional<JedisPool> jedisPool(RedisProperties props) {
        System.out.println("[REDIS-FACTORY] jedisPool() called with props.enabled=" + props.enabled);
        
        if (!props.enabled) {
            System.out.println("[REDIS-FACTORY] Redis disabled, returning empty Optional");
            return Optional.empty();
        }

        try {
            System.out.println("[REDIS-FACTORY] Creating JedisPool from: " + props.url);
            log.info("[REDIS-FACTORY] Initializing JedisPool from URL: {}", props.url);

            URI redisUri = new URI(props.url);
            String host = redisUri.getHost() != null ? redisUri.getHost() : "localhost";
            int port = redisUri.getPort() > 0 ? redisUri.getPort() : 6379;
            
            String password = null;
            if (redisUri.getUserInfo() != null) {
                String[] userInfo = redisUri.getUserInfo().split(":");
                if (userInfo.length == 2) {
                    password = userInfo[1];
                }
            }

            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(props.connectionPoolSize);
            poolConfig.setMaxIdle(props.connectionPoolSize / 2);
            poolConfig.setMinIdle(2);
            poolConfig.setTestOnBorrow(true);
            poolConfig.setTestOnReturn(true);
            poolConfig.setTestWhileIdle(true);
            poolConfig.setMinEvictableIdleTimeMillis(60000);
            poolConfig.setTimeBetweenEvictionRunsMillis(30000);
            poolConfig.setNumTestsPerEvictionRun(3);
            poolConfig.setBlockWhenExhausted(true);

            JedisPool pool = new JedisPool(
                poolConfig,
                host,
                port,
                props.timeoutMs,
                password,
                props.database
            );

            try (Jedis jedis = pool.getResource()) {
                String pong = jedis.ping();
                log.info("[REDIS-FACTORY] ✓ Connected to Redis at {}:{}", host, port);
                System.out.println("[REDIS-FACTORY] ✓ Connected to Redis at " + host + ":" + port);
            }

            System.out.println("[REDIS-FACTORY] JedisPool created successfully, returning Optional.of(pool)");
            return Optional.of(pool);

        } catch (Exception e) {
            log.error("[REDIS-FACTORY] ✗ Failed to initialize JedisPool: {}", e.getMessage(), e);
            System.out.println("[REDIS-FACTORY] ✗ Failed: " + e.getMessage());
            e.printStackTrace();
            return Optional.empty();
        }
    }
}
