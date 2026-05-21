# OAuth2 Authentication Manager Wiring Fix - Summary

## 🎯 Problem
When attempting social login (Facebook, GitHub), the OAuth2 flow was not calling `SocialLoginService.loadUser()` as expected. Instead, it would:
1. Jump directly to `SocialLoginSuccessHandler` with incomplete user info
2. Result in `email=null` for GitHub logins (couldn't fetch email from GitHub API)
3. Occasionally throw error: `IllegalStateException: No provider found for class OAuth2AuthorizationCodeAuthenticationToken`

## ✅ Root Cause
**SecurityConfig.java** was incorrectly configured:
```java
// BEFORE (WRONG)
.authenticationManager(socialLoginService)
```

`SocialLoginService` implements `DefaultReactiveOAuth2UserService` (a user info service), NOT `ReactiveAuthenticationManager` (authentication orchestrator).

Spring Security WebFlux **must have** a proper authentication manager to:
1. Exchange authorization code for access token
2. Load user info using the access token
3. Create an authenticated token for success handler

## 🔧 Solution
Created proper `oAuth2LoginAuthenticationManager()` bean in **SecurityConfig.java**:

### What it does:
```
Authorization Code ↓
    ↓
Exchange code → access token  (WebClientReactiveAuthorizationCodeTokenResponseClient)
    ↓
Load user info with token  (socialLoginService.loadUser())
    ↓
Create OAuth2LoginAuthenticationToken with full user data
    ↓
Pass to SocialLoginSuccessHandler (which sets cookies and redirects)
```

### Code Changes:
```java
@Bean
public ReactiveAuthenticationManager oAuth2LoginAuthenticationManager() {
    ReactiveOAuth2AccessTokenResponseClient<OAuth2AuthorizationCodeGrantRequest> 
        tokenResponseClient = new WebClientReactiveAuthorizationCodeTokenResponseClient();
    
    return authenticationToken -> {
        if (!(authenticationToken instanceof OAuth2AuthorizationCodeAuthenticationToken)) {
            return Mono.error(new IllegalArgumentException("Invalid authentication token type"));
        }
        
        OAuth2AuthorizationCodeAuthenticationToken codeToken = 
            (OAuth2AuthorizationCodeAuthenticationToken) authenticationToken;
        
        // Convert to grant request
        OAuth2AuthorizationCodeGrantRequest grantRequest = 
            new OAuth2AuthorizationCodeGrantRequest(
                codeToken.getClientRegistration(),
                codeToken.getAuthorizationExchange()
            );
        
        // Exchange code for token, then load user
        return tokenResponseClient.getTokenResponse(grantRequest)
            .flatMap(tokenResponse -> 
                // This is the KEY: call SocialLoginService with access token
                socialLoginService.loadUser(
                    new OAuth2UserRequest(
                        codeToken.getClientRegistration(),
                        tokenResponse.getAccessToken()
                    )
                )
                .map(oAuth2User -> new OAuth2LoginAuthenticationToken(
                    codeToken.getClientRegistration(),
                    codeToken.getAuthorizationExchange(),
                    oAuth2User,
                    oAuth2User.getAuthorities(),
                    tokenResponse.getAccessToken(),
                    tokenResponse.getRefreshToken()
                ))
            );
    };
}
```

In `filterChain()`:
```java
serverHttpSecurity.oauth2Login(oauth2 -> oauth2
    .clientRegistrationRepository(reactiveClientRegistrationRepository())
    .authenticationManager(oAuth2LoginAuthenticationManager())  // ← Use custom manager
    .authenticationSuccessHandler(socialLoginSuccessHandler)
    .authenticationFailureHandler(socialLoginFailService)
);
```

## 📊 Before vs After

| Aspect | Before | After |
|--------|--------|-------|
| **SocialLoginService.loadUser() called?** | ❌ NO | ✅ YES |
| **GitHub email extraction** | ❌ email=null | ✅ Fetched from API |
| **User creation from social** | ❌ Failed (missing email) | ✅ Succeeds |
| **Error handling** | ❌ "No provider found" | ✅ Proper error messages |

## 🧪 Testing the Fix

### 1. GitHub Login Flow
```
1. Click "Login with GitHub"
2. Authorize app
3. Redirected to callback with authorization code
4. Backend exchanges code for access token
5. SocialLoginService.loadUser() called with access token
6. GitHub email fetched from /user/emails endpoint
7. User created in database
8. Cookies set (accessToken, refreshToken, deviceInfo)
9. Redirected to frontend with authenticated session
```

### 2. Expected Logs
```
SocialLoginService | Successfully loaded user from provider: {... email: "user@example.com", ...}
SocialLoginService | Processing user from provider: email=user@example.com, displayName=Bach Dau, provider=github
SocialLoginService | Successfully created new user from social media: {...}
SocialLoginService | Successfully fetched email from GitHub: user@example.com
```

### 3. Verification
- ✅ `mvn clean compile` - compiles without errors
- ✅ `mvn clean install` - packages successfully
- ✅ Application starts without bean circular dependency errors
- ✅ Social login doesn't throw "No provider found" exception

## 📝 Files Modified

- **SecurityConfig.java**
  - Added imports for OAuth2 beans
  - Created `oAuth2LoginAuthenticationManager()` bean
  - Updated `.oauth2Login()` configuration to use custom manager

- **CHANGELOG.md**
  - Added entry documenting the fix with explanation and results

## 🎓 Key Learning

The distinction between:
- **User Service** (e.g., `DefaultReactiveOAuth2UserService`) - loads user info from provider
- **Authentication Manager** (e.g., `ReactiveAuthenticationManager`) - orchestrates the auth flow

Spring Security requires both:
1. Authentication manager handles the protocol flow (code exchange)
2. User service loads user data at the right time

Simply providing a user service as an authentication manager breaks the flow!

## ⚠️ Dependencies

The fix requires these imports:
```java
import org.springframework.security.authentication.ReactiveAuthenticationManager;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthorizationCodeAuthenticationToken;
import org.springframework.security.oauth2.client.authentication.OAuth2LoginAuthenticationToken;
import org.springframework.security.oauth2.client.endpoint.OAuth2AuthorizationCodeGrantRequest;
import org.springframework.security.oauth2.client.endpoint.ReactiveOAuth2AccessTokenResponseClient;
import org.springframework.security.oauth2.client.endpoint.WebClientReactiveAuthorizationCodeTokenResponseClient;
```

All are part of `spring-security-oauth2-client` (already in pom.xml).

