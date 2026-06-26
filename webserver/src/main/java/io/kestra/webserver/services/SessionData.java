package io.kestra.webserver.services;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents session data stored by the frontend Astro application and validated by the backend.
 * Stores JWT tokens and session metadata for SSO authentication.
 * 
 * This class is synchronized between frontend SessionStore and backend SessionStoreService.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SessionData {
    /**
     * OAuth2 access token containing user claims and roles.
     * Format: JWT token from Keycloak server.
     */
    private String accessToken;

    /**
     * OAuth2 refresh token for renewing access token.
     * Format: JWT token from Keycloak server.
     */
    private String refreshToken;

    /**
     * OpenID Connect ID token containing user identity information.
     * Format: JWT token from Keycloak server.
     */
    private String idToken;

    /**
     * Subject claim (unique user identifier) from the token.
     * Format: UUID from Keycloak.
     */
    private String userSub;

    /**
     * Timestamp when this session was created.
     * Format: milliseconds since epoch.
     */
    private long createdAt;

    /**
     * Timestamp when the current access_token expires.
     * Typically 5-15 minutes after token refresh.
     * Format: milliseconds since epoch.
     */
    private long accessTokenExpiresAt;

    /**
     * Timestamp when this session truly expires (based on Keycloak refresh_token lifetime).
     * Typically 1 hour or 24 hours depending on Keycloak config.
     * Format: milliseconds since epoch.
     */
    private long sessionExpiresAt;

    /**
     * Deprecated: use accessTokenExpiresAt instead.
     * Kept for backward compatibility.
     */
    @Deprecated
    private long expiresAt;

    /**
     * Backward compatibility getter.
     */
    public long getExpiresAt() {
        return accessTokenExpiresAt > 0 ? accessTokenExpiresAt : expiresAt;
    }

    /**
     * Backward compatibility setter.
     */
    public void setExpiresAt(long expiresAt) {
        this.accessTokenExpiresAt = expiresAt;
        this.expiresAt = expiresAt;
    }

    /**
     * User email address from Keycloak token or ID token.
     * Format: email string from Keycloak.
     */
    private String email;

    /**
     * Getter for email.
     */
    public String getEmail() {
        return email;
    }

    /**
     * Setter for email.
     */
    public void setEmail(String email) {
        this.email = email;
    }
}
