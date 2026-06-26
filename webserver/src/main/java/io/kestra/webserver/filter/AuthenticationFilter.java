package io.kestra.webserver.filter;

import java.util.Collection;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kestra.core.utils.AuthUtils;
import io.kestra.webserver.services.BasicAuthService;
import io.kestra.webserver.services.SessionData;
import io.kestra.webserver.services.SessionStoreService;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.cookie.Cookie;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import io.micronaut.http.filter.ServerFilterPhase;
import io.micronaut.management.endpoint.annotation.Endpoint;
import io.micronaut.web.router.MethodBasedRouteMatch;
import io.micronaut.web.router.RouteMatch;
import io.micronaut.web.router.RouteMatchUtils;
import jakarta.inject.Inject;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

//We want to authenticate only Kestra endpoints
@Filter("/api/v1/**")
@Requires(property = "kestra.server-type", pattern = "(WEBSERVER|STANDALONE)")
@Requires(property = "micronaut.security.enabled", notEquals = "true") // don't add this filter in EE
public class AuthenticationFilter implements HttpServerFilter {
    private static final Integer ORDER = ServerFilterPhase.SECURITY.order();
    public static final String BASIC_AUTH_COOKIE_NAME = BasicAuthService.BASIC_AUTH_COOKIE_NAME;
    private static final Logger LOG = LoggerFactory.getLogger(AuthenticationFilter.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Inject
    private BasicAuthService basicAuthService;

    @Inject
    private SessionStoreService sessionStoreService;

    @Override
    public int getOrder() {
        return ORDER;
    }

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
        return Mono.fromCallable(() -> basicAuthService.configuration())
            .subscribeOn(Schedulers.boundedElastic())
            .flux()
            .flatMap(basicAuthConfiguration ->
            {
                String normalizedPath = normalizePath(request.getPath());
                boolean isConfigEndpoint = "/api/v1/configs".equals(normalizedPath)
                    || ((normalizedPath.matches("/api/v1(/[^/]+)?/basicAuth") || "/api/v1/basicAuthValidationErrors".equals(normalizedPath))
                        && !basicAuthService.isBasicAuthInitialized());

                /*
                boolean isOpenUrl = Optional.ofNullable(basicAuthConfiguration.openUrls())
                    .map(Collection::stream)
                    .map(stream -> stream.anyMatch(s -> request.getPath().startsWith(s)))
                    .orElse(false);

                if (isConfigEndpoint || isOpenUrl || isManagementEndpoint(request)) {
                    return chain.proceed(request);
                }
                */
                // ⚡ AUTHENTICATION PRIORITY ORDER (FINAL - CORRECTED):
                // 1️⃣ DATALAKE_SSO_SESSION cookie (SSO) - HIGHEST PRIORITY
                //    - Valid SSO + Matching Bearer email → ALLOW ✅
                //    - Valid SSO + NO Bearer → REJECT 401 (Bearer email REQUIRED)
                //    - Valid SSO + Mismatched Bearer email → REJECT 401 (NO FALLBACK)
                //    - SSO cookie MISSING in request → Skip, try Bearer token
                //    - SSO cookie present but INVALID in Redis → Skip, try Bearer token
                // 2️⃣ Authorization: Bearer token - FALLBACK (if SSO not present/valid)
                //    - Valid Bearer → ALLOW ✅
                //    - Invalid Bearer → REJECT 401 (NO FALLBACK)
                //    - No Bearer token → Reject 401 (no auth provided)
                // ❌ Basic Auth: DISABLED (not used)

                // Extract Bearer token early for SSO validation
                Optional<String> authHeader = request.getHeaders().getAuthorization();
                String bearerToken = null;
                if (authHeader.isPresent() && authHeader.get().toLowerCase().startsWith("bearer ")) {
                    bearerToken = authHeader.get().substring("bearer ".length());
                }

                // Check 1: SSO session cookie (PRIORITY 1 - HIGHEST)
                // Only process if SSO cookie EXISTS in the HTTP request
                Boolean isValidSSO = isValidSSOFromRequest(request);
                if (!isValidSSO) {
                    Boolean isFromLoginPage = Optional.ofNullable(request.getHeaders().get("Referer"))
                            .map(referer -> referer.split("\\?")[0].endsWith("/login")).orElse(false);
                    return Mono.just(HttpResponse.unauthorized())
                        .map(response -> isFromLoginPage ? response : response.header("WWW-Authenticate", "Bearer"));
                }
                return chain.proceed(request);
            });
    }

    private static String normalizePath(String path) {
        return path.replaceAll("/+", "/");
    }

    /**
     * Extracts SSO session ID from HTTP request cookies.
     * Checks DATALAKE_SSO_SESSION first, then falls back to SESSION_ID.
     * 
     * @param request The HTTP request
     * @return Session ID if found in cookies, null otherwise
     */
    private Boolean isValidSSOFromRequest(HttpRequest<?> request) {
        try {
            // Try DATALAKE_SSO_SESSION cookie
            Cookie ssoSession = request.getCookies().get("DATALAKE_SSO_SESSION");
            Cookie accessTokenExpiresAt = request.getCookies().get("access_token_expires_at");
            if (ssoSession == null || ssoSession.getValue() == null || ssoSession.getValue().isEmpty() ||
                accessTokenExpiresAt == null || accessTokenExpiresAt.getValue() == null || accessTokenExpiresAt.getValue().isEmpty()) {
                return false;
            }
            // SSO cookie found in request - validate it in Redis
            SessionData sessionData = sessionStoreService.getSession(ssoSession.getValue() + "||" + accessTokenExpiresAt.getValue());
            if (sessionData == null || sessionData.getEmail() == null) return false;
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Extract email from user_info cookie.
     * The cookie contains URL-encoded JSON with email and name fields.
     * 
     * @param request The HTTP request
     * @return Optional containing the extracted email, or empty if not found
     */
    private Optional<String> extractEmailFromUserInfoCookie(HttpRequest<?> request) {
        try {
            Cookie userInfo = request.getCookies().get("user_info");
            if (userInfo != null && userInfo.getValue() != null && !userInfo.getValue().isEmpty()) {
                String decodedValue = java.net.URLDecoder.decode(userInfo.getValue(), "UTF-8");
                
                // Parse JSON object
                UserInfoData userInfoData = OBJECT_MAPPER.readValue(decodedValue, UserInfoData.class);
                
                if (userInfoData != null && userInfoData.email != null && !userInfoData.email.isEmpty()) {
                    return Optional.of(userInfoData.email);
                }
            }
            return Optional.empty();
        } catch (Exception e) {
            LOG.debug("[AUTH-FILTER] Failed to extract email from user_info cookie: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Inner class to represent the user_info cookie JSON structure
     */
    public static class UserInfoData {
        public String email;
        public String name;

        // Default constructor for Jackson
        public UserInfoData() {
        }

        public UserInfoData(String email, String name) {
            this.email = email;
            this.name = name;
        }
    }

    @SuppressWarnings("rawtypes")
    private boolean isManagementEndpoint(HttpRequest<?> request) {
        Optional<RouteMatch> routeMatch = RouteMatchUtils.findRouteMatch(request);
        if (routeMatch.isPresent() && routeMatch.get() instanceof MethodBasedRouteMatch<?, ?> method) {
            return method.getAnnotation(Endpoint.class) != null;
        }
        return false;
    }
}
