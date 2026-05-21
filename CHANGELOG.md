> Quy ước: mỗi lần thay đổi code liên quan đến auth/security/cookie/refresh-token, hãy **append thêm một mục mới ở cuối file** này.

## 2026-05-08 - Initial HttpOnly token storage + refresh flow

### Mục tiêu
- Chuyển luồng auth sang lưu `accessToken`, `refreshToken`, `deviceInfo` vào **HttpOnly cookie**.
- Không còn truyền token qua query params.
- Khi `accessToken` hết hạn, frontend sẽ nhận `401` với mã lỗi rõ ràng để gọi `POST /auth/refresh-token`.
- Nếu refresh lỗi, backend sẽ revoke toàn bộ refresh token đang active của user và trả về response có mã lỗi + message cụ thể.

### Các file đã thay đổi
- `src/main/java/com/dauducbach/clone/modules/auth/controller/AuthenticationController.java`
  - Thêm controller auth WebFlux.
  - `POST /auth/login`: set HttpOnly cookies sau khi login thành công.
  - `POST /auth/refresh-token`: đọc refresh token/deviceInfo từ cookie hoặc body, refresh và set cookies mới.
  - `POST /auth/logout`: revoke token và clear cookies.
  - `POST /auth/introspect`: giữ endpoint introspect.

- `src/main/java/com/dauducbach/clone/modules/auth/service/AuthCookieService.java`
  - Helper dùng chung để set/clear cookie HttpOnly.
  - Hỗ trợ resolve cookie từ `ServerWebExchange`.
  - Đồng bộ cách set cookie cho login/refresh/social login.

- `src/main/java/com/dauducbach/clone/modules/auth/service/AuthenticationService.java`
  - Login nhận `deviceInfo` từ FE và lưu vào refresh token record.
  - Refresh token flow được refactor để:
    - kiểm tra refresh token hợp lệ
    - revoke token cũ
    - tạo access token mới
    - lưu refresh token mới
    - rollback/revoke toàn bộ refresh token active nếu có lỗi trong quá trình refresh
  - Trả message lỗi cụ thể cho user bằng `AppException` detail message.

- `src/main/java/com/dauducbach/clone/modules/auth/service/SocialLoginSuccessHandler.java`
  - Không còn đưa token vào query params.
  - Set cookies HttpOnly rồi redirect sang frontend.
  - Đồng bộ với cookie helper.

- `src/main/java/com/dauducbach/clone/configuration/SecurityConfig.java`
  - Bật CORS cho WebFlux security với `allowCredentials=true`.
  - Permit các endpoint auth cần thiết.
  - Thêm `authenticationEntryPoint` để trả mã lỗi riêng cho `accessToken` hết hạn.
  - OAuth2 login dùng social login success handler để cũng đi theo flow HttpOnly.

- `src/main/java/com/dauducbach/clone/modules/auth/dto/request/LoginRequest.java`
  - Thêm `deviceInfo` để FE gửi lên khi login.

- `src/main/java/com/dauducbach/clone/modules/auth/repository/RefreshTokensRepository.java`
  - Thêm query revoke toàn bộ refresh token active theo `userId` để rollback khi refresh lỗi.

- `src/main/java/com/dauducbach/clone/commons/exception/AppException.java`
  - Hỗ trợ custom detail message và cause.

- `src/main/java/com/dauducbach/clone/commons/exception/ErrorCode.java`
  - Thêm `ACCESS_TOKEN_EXPIRED`.

- `src/main/java/com/dauducbach/clone/commons/exception/GlobalExceptionHandler.java`
  - Ưu tiên hiển thị message chi tiết từ `AppException`.

### Hành vi mới
- Login thành công:
  - set cookie `accessToken`
  - set cookie `refreshToken`
  - set cookie `deviceInfo`
- Refresh thành công:
  - set lại cả 3 cookie
- Refresh lỗi:
  - revoke toàn bộ refresh token active của user
  - trả `AppException` với message rõ ràng
- Access token hết hạn:
  - security trả `401` với mã lỗi riêng để FE biết gọi refresh endpoint

### Ghi chú cho các lần thay đổi sau
- Khi sửa code tiếp theo, hãy **thêm một section mới xuống cuối file**.
- Nên ghi rõ:
  - mục tiêu thay đổi
  - file nào đã sửa
  - hành vi mới
  - lưu ý/ảnh hưởng tới frontend/backend

## 2026-05-08 - Follow-up: cookie helper injection fix + fail-fast cleanup

### Mục tiêu
- Sửa lại cách inject `jwt.valid-duration` trong `AuthCookieService` để tránh lỗi autowire primitive `long`.
- Làm rõ hơn luồng fail-fast cho refresh/logout: nếu thiếu cookie/body đầu vào thì clear cookie cũ ngay.

### Các file đã tinh chỉnh thêm
- `src/main/java/com/dauducbach/clone/modules/auth/service/AuthCookieService.java`
  - Đổi sang inject `jwt.valid-duration` bằng `@Value` field thường.
  - Tăng độ an toàn khi đọc cookie từ request.

- `src/main/java/com/dauducbach/clone/modules/auth/controller/AuthenticationController.java`
  - Clear cookie stale ngay khi refresh/logout request thiếu dữ liệu bắt buộc.

### Hành vi cập nhật
- Cookie helper ổn định hơn với primitive config.
- Nếu frontend gọi refresh/logout mà thiếu `accessToken`/`refreshToken`/`deviceInfo`, backend sẽ clear cookie cũ thay vì giữ trạng thái lỗi.

## 2026-05-08 - Cleanup: remove legacy OAuth2 JSON success handler

### Mục tiêu
- Loại bỏ bean success handler JSON cũ trong `SecurityConfig` vì social login đã chuyển sang lưu cookie HttpOnly và redirect sạch.

### File đã tinh chỉnh
- `src/main/java/com/dauducbach/clone/configuration/SecurityConfig.java`
  - Bỏ bean `ServerAuthenticationSuccessHandler` kiểu cũ.
  - Giữ social login success handler cookie-based làm handler chính.

### Hành vi cập nhật
- Không còn bất kỳ flow OAuth2 nào trả token qua body/URL theo handler JSON cũ.

## 2026-05-08 - Scope update: log mọi thay đổi code vào cùng một file

### Mục tiêu
- Làm rõ rằng file changelog này không chỉ dành cho Auth HttpOnly, mà là nơi **ghi lại toàn bộ thay đổi code trong project** từ bây giờ trở đi.

### Quy ước mới
- Mỗi lần sửa bất kỳ file code nào trong project, hãy **append thêm một section mới ở cuối file** này.
- Ghi rõ:
  - mục tiêu thay đổi
  - file nào đã sửa
  - hành vi mới
  - lưu ý ảnh hưởng tới các phần khác nếu có

### Áp dụng cho tương lai
- Nếu sau này sửa `SecurityConfig`, `AuthenticationService`, `SocialLoginFailService`, controller, DTO, repository, hay bất kỳ file nào khác, đều phải cập nhật tiếp vào file changelog này.

## 2026-05-08 - Add UserCredentials controller APIs

### Mục tiêu
- Tạo controller WebFlux cho toàn bộ API của `UserCredentialsService`.

### Các file đã thêm/sửa
- `src/main/java/com/dauducbach/clone/modules/auth/controller/UserCredentialsController.java`
  - Thêm các endpoint:
    - `POST /auth/user-credentials/pre-register`
    - `POST /auth/user-credentials/email-verify-and-create-user`
    - `POST /auth/user-credentials/check-and-send-code-for-forget-password`
    - `POST /auth/user-credentials/verify-and-send-new-password-to-user`
    - `POST /auth/user-credentials/verify-and-send-new-username-and-new-password-to-user`

- `src/main/java/com/dauducbach/clone/modules/auth/dto/request/SendCodeForForgetPasswordRequest.java`
  - Thêm request body riêng cho API quên mật khẩu.

- `src/main/java/com/dauducbach/clone/modules/auth/dto/request/CreateUserRequest.java`
- `src/main/java/com/dauducbach/clone/modules/auth/dto/request/EmailVerifyRequest.java`
  - Bổ sung validation để controller dùng `@Valid` có ý nghĩa.

- `src/main/java/com/dauducbach/clone/configuration/SecurityConfig.java`
  - Cho phép public các endpoint mới của `UserCredentialsController`.

### Hành vi mới
- FE có thể gọi trực tiếp các API user-credentials qua JSON body.
- Các lỗi nghiệp vụ vẫn đi qua `AppException`/`GlobalExceptionHandler`.

## 2026-05-08 - Add SocialLoginFailService for frontend OAuth2 error response

### Mục tiêu
- Hoàn thiện handler lỗi cho social login để frontend nhận được JSON lỗi rõ ràng khi đăng nhập social thất bại.

### Các file đã thêm/sửa
- `src/main/java/com/dauducbach/clone/modules/auth/service/SocialLoginFailService.java`
  - Implement `ServerAuthenticationFailureHandler` cho WebFlux social login.
  - Trả response `401` dạng JSON có `code`, `message`, `traceId`.
  - Clear auth cookies trước khi trả lỗi để tránh giữ trạng thái cũ ở browser.

- `src/main/java/com/dauducbach/clone/configuration/SecurityConfig.java`
  - Gắn `SocialLoginFailService` vào `oauth2Login().authenticationFailureHandler(...)`.
  - Bỏ handler thất bại kiểu cũ không còn dùng.

### Hành vi mới
- Khi social login thất bại, frontend sẽ nhận JSON lỗi thống nhất thay vì `null` hoặc response mơ hồ.
- Có thể phân biệt lỗi thiếu thông tin user social với lỗi load user social từ provider.

## 2026-05-08 - Fix oauth2Login clientRegistrationRepository null

### Mục tiêu
- Sửa lỗi `clientRegistrationRepository cannot be null` khi khởi tạo `SecurityWebFilterChain`.

### Các file đã sửa
- `src/main/java/com/dauducbach/clone/configuration/SecurityConfig.java`
  - Bật `@EnableWebFluxSecurity`.
  - Tạo `ReactiveClientRegistrationRepository` thủ công cho Google/Facebook/GitHub.
  - Gắn repository này vào `oauth2Login(...)`.

### Hành vi mới
- `oauth2Login()` không còn fail vì thiếu `clientRegistrationRepository`.
- Social login có thể khởi tạo đúng flow reactive.

## 2026-05-10 - Notification fan-out: one recipient per delivery service

### Mục tiêu
- Tách logic nhận payload notification từ Kafka ra khỏi logic gửi cho từng người nhận.
- `NotificationService` sẽ fan-out theo danh sách recipient, đọc `UserNotificationSetting` trước khi gửi.
- `EmailService` và `PushNotificationService` chỉ xử lý **một recipient duy nhất** mỗi lần gọi.

### Các file đã thay đổi
- `src/main/java/com/dauducbach/clone/modules/notification/dto/NotificationForService.java`
  - Đổi `recipientIds: List<String>` thành `recipient: String` để dùng cho từng lần gửi đơn lẻ.

- `src/main/java/com/dauducbach/clone/modules/notification/service/NotificationService.java`
  - Nhận payload có danh sách recipients.
  - Với từng recipient:
    - load `UserNotificationSetting`
    - kiểm tra setting theo channel (`emailNotification` / `pushNotification`)
    - kiểm tra setting theo loại action (`LIKE`, `COMMENT`, `SEND_MESSAGE`)
    - dispatch sang `EmailService` hoặc `PushNotificationService`
  - Nếu thiếu setting thì dùng default allow-all để không làm gián đoạn luồng hiện tại.

- `src/main/java/com/dauducbach/clone/modules/notification/service/EmailService.java`
  - Chỉ gửi cho 1 recipient/userId mỗi lần.
  - Tự resolve email của recipient qua `UserCredentialsRepository`.
  - Sau khi gửi mail thành công thì persist `NotificationEvents` và `UserNotifications`.

- `src/main/java/com/dauducbach/clone/modules/notification/service/PushNotificationService.java`
  - Chỉ gửi push cho 1 recipient/userId mỗi lần.
  - Giữ logic lấy `NotificationPushToken`, gửi Firebase và lưu DB.

- `src/main/java/com/dauducbach/clone/commons/exception/ErrorCode.java`
  - Thêm `NOTIFICATION_TYPE_NOT_SUPPORTED` cho trường hợp payload dùng loại notification chưa hỗ trợ.

### Hành vi mới
- Kafka payload có thể chứa nhiều recipients.
- Notification sẽ được xử lý theo từng recipient riêng lẻ.
- Setting của user sẽ được kiểm tra trước khi gọi xuống service gửi email/push.
- Service gửi email/push không còn tự fan-out nữa.

### Ghi chú
- Đây là bước refactor nền tảng cho luồng notification fan-out.
- Nếu sau này cần thêm listener Kafka cụ thể hoặc hỗ trợ SMS, hãy append tiếp section mới ở cuối file này.

## 2026-05-10 - Follow-up: cleanup NotificationService fan-out chain

### Mục tiêu
- Làm sạch lại chuỗi reactive trong `NotificationService` để loại bỏ các vấn đề type inference/compile warning còn sót lại sau lần refactor fan-out.

### Các file đã tinh chỉnh thêm
- `src/main/java/com/dauducbach/clone/modules/notification/service/NotificationService.java`
  - Giữ logic fan-out theo từng recipient.
  - Làm rõ chuỗi `Mono` khi iterate recipients.
  - Loại bỏ các generic type argument không cần thiết.

### Hành vi cập nhật
- Luồng xử lý notification theo từng recipient vẫn giữ nguyên.
- `NotificationService` đã sạch hơn về mặt compile/type inference trong IDE.

## 2026-05-10 - Notification settings simplified: channel-only gating

### Mục tiêu
- Chỉ dùng 2 cờ cấu hình `pushNotification` và `emailNotification` để quyết định có gửi hay không.

### Các file đã thay đổi
- `src/main/java/com/dauducbach/clone/modules/notification/service/NotificationService.java`
  - Bỏ toàn bộ logic chặn theo action type (`LIKE`, `COMMENT`, `SEND_MESSAGE`).
  - Với `EMAIL` chỉ check `emailNotification`.
  - Với `PUSH` chỉ check `pushNotification`.

### Hành vi mới
- Nếu user bật `emailNotification` thì gửi email, tắt thì bỏ qua.
- Nếu user bật `pushNotification` thì gửi push, tắt thì bỏ qua.
- Các cờ chi tiết khác trong `UserNotificationSetting` hiện không ảnh hưởng đến quyết định gửi.

## 2026-05-20 - Add ACTION_GUIDE for auth logout/refresh testing

### Mục tiêu
- Tạo một file hướng dẫn thao tác/test luồng logout và refresh token cho cả Postman và Frontend.

### Các file đã thêm/sửa
- `ACTION_GUIDE.md`
  - Mô tả base URL thực tế theo `server.port=8888` và `spring.webflux.base-path=/app`.
  - Hướng dẫn test `login`, `refresh-token`, `logout`, `introspect` bằng Postman.
  - Hướng dẫn FE gọi API với `credentials` / `withCredentials` để browser tự gửi cookie HttpOnly.
  - Có ví dụ axios interceptor để tự refresh khi gặp `401`.

### Hành vi mới
- Có một tài liệu tập trung để debug/test luồng auth mà không phải đọc lại code mỗi lần.
- Khi có thay đổi auth/security/cookie/refresh-token trong tương lai, tiếp tục append vào file này.

## 2026-05-21 - Wire SocialLoginService into OAuth2 login manager

### Mục tiêu
- Đảm bảo `SocialLoginService.loadUser()` luôn được thực thi trong OAuth2 login flow trước khi gọi `SocialLoginSuccessHandler`.

### Các file đã sửa
- `src/main/java/com/dauducbach/clone/configuration/SecurityConfig.java`
  - Inject thêm `SocialLoginService` vào `SecurityConfig`.
  - Khai báo bean `ReactiveOAuth2AccessTokenResponseClient<OAuth2AuthorizationCodeGrantRequest>`.
  - Khai báo bean `ReactiveAuthenticationManager` bằng `OAuth2LoginReactiveAuthenticationManager` và gắn `socialLoginService`.
  - Cấu hình `oauth2Login(...).authenticationManager(...)` để Spring dùng custom `SocialLoginService` khi load user social.

### Hành vi mới
- Luồng `/oauth2/authorization/{provider}` sau callback sẽ dùng `SocialLoginService.loadUser()` để load/process user từ provider.
- Sau khi xác thực thành công mới chuyển sang `SocialLoginSuccessHandler` để xử lý bước success (set cookie/redirect).

## 2026-05-21 - Fix circular dependency SecurityConfig <-> SocialLoginService

### Mục tiêu
- Sửa lỗi startup `The dependencies of some of the beans in the application context form a cycle`.

### Các file đã sửa
- `src/main/java/com/dauducbach/clone/modules/auth/service/SocialLoginService.java`
  - Bỏ dependency `PasswordEncoder` không sử dụng.

### Nguyên nhân và kết quả
- Do `SocialLoginService` phụ thuộc `PasswordEncoder` bean (được khai báo trong `SecurityConfig`) trong khi `SecurityConfig` cũng phụ thuộc `SocialLoginService`, tạo vòng lặp bean.
- Sau khi bỏ dependency không dùng này, vòng phụ thuộc được phá và context có thể khởi tạo bình thường.

## 2026-05-21 - Fix GitHub OAuth2 email extraction

### Mục tiêu
- GitHub API trả `email=null` trong user info endpoint mặc định.
- Cần fetch email từ endpoint `/user/emails` riêng hoặc sử dụng primary/verified email.

### Các file đã sửa
- `src/main/java/com/dauducbach/clone/modules/auth/service/SocialLoginService.java`
  - Sửa method `fetchGithubEmail()`:
    - Đổi header `Authorization` từ `Bearer <token>` sang `token <token>` (GitHub yêu cầu format này).
    - Thêm filter cho `primary` email ngoài `verified` email.
    - Cải thiện error handling với `onErrorResume()`.

- `src/main/java/com/dauducbach/clone/configuration/SecurityConfig.java`
  - Thêm bean `WebClient` để hỗ trợ gọi GitHub API từ `SocialLoginService`.
  - Đã có scope `user:email` được set ở GitHub client registration (dòng 184).

### Hành vi mới
- Khi user login bằng GitHub, nếu email null:
  1. `SocialLoginService` sẽ gọi `fetchGithubEmail()` để lấy từ `/user/emails` endpoint.
  2. Lấy email đầu tiên có `verified=true` hoặc `primary=true`.
  3. Nếu thất bại, log warning và tiếp tục (có thể fail khi tạo user nếu không có email).

### Test
- Khi login GitHub: Nên thấy log `Successfully fetched email from GitHub: <email>` thay vì email=null.

## 2026-05-21 - Fix OAuth2 "No provider found" error

### Mục tiêu
- Khi login qua Facebook/GitHub, nhận lỗi: `IllegalStateException: No provider found for class org.springframework.security.oauth2.client.authentication.OAuth2AuthorizationCodeAuthenticationToken`
- Nguyên nhân: Custom `authenticationManager()` override làm bypass Spring Security's built-in OAuth2 authentication chain.

### Các file đã sửa
- `src/main/java/com/dauducbach/clone/configuration/SecurityConfig.java`
  - **Xóa** `.authenticationManager(oAuth2LoginAuthenticationManager())` từ `oauth2Login()` config
  - **Xóa** bean `ReactiveOAuth2AccessTokenResponseClient<OAuth2AuthorizationCodeGrantRequest> authorizationCodeTokenResponseClient()`
  - **Xóa** bean `ReactiveAuthenticationManager oAuth2LoginAuthenticationManager()`
  - **Xóa** imports: `ReactiveAuthenticationManager`, `OAuth2LoginReactiveAuthenticationManager`, `OAuth2AuthorizationCodeGrantRequest`, `ReactiveOAuth2AccessTokenResponseClient`, `WebClientReactiveAuthorizationCodeTokenResponseClient`

### Nguyên nhân
- Khi custom `authenticationManager`, Spring không properly register provider cho `OAuth2AuthorizationCodeAuthenticationToken`.
- Solution là rely on Spring's default OAuth2 authentication flow, để Spring auto-detect `SocialLoginService` (extends `DefaultReactiveOAuth2UserService`) từ bean context.

### Hành vi mới
- OAuth2 login flow sử dụng Spring's default manager + auto-detected `SocialLoginService` bean.
- `SocialLoginService.loadUser()` sẽ tự động được gọi trước success handler.
- Khỏi phải manual wiring custom authentication manager.

### Kết quả
- **Trước**: `IllegalStateException: No provider found`
- **Sau**: OAuth2 login flow hoạt động bình thường, `SocialLoginService.loadUser()` được gọi, sau đó `SocialLoginSuccessHandler` xử lý success.

## 2026-05-21 - Fix OAuth2 authentication manager wiring (SocialLoginService not being called)

### Mục tiêu
- Fix vấn đề: OAuth2 login success handler được gọi trực tiếp mà không đi qua `SocialLoginService.loadUser()`.
- Đảm bảo OAuth2 flow hoạt động đúng: Authorization Code → Token Exchange → User Load → Success Handler.
- Tránh lỗi `IllegalStateException: No provider found for class OAuth2AuthorizationCodeAuthenticationToken`.

### Nguyên nhân
- SecurityConfig.java cấu hình sai: `.authenticationManager(socialLoginService)`.
- `SocialLoginService` implement `DefaultReactiveOAuth2UserService` (user service), không phải `ReactiveAuthenticationManager`.
- Spring Security WebFlux cần authentication manager để xử lý OAuth2 authorization code flow trước khi load user info.

### Các file đã thay đổi

**`src/main/java/com/dauducbach/clone/configuration/SecurityConfig.java`**
- Thêm import: `OAuth2AuthorizationCodeGrantRequest`, `ReactiveOAuth2AccessTokenResponseClient`, `WebClientReactiveAuthorizationCodeTokenResponseClient`.
- Tạo bean `oAuth2LoginAuthenticationManager()`:
  - Tạo `WebClientReactiveAuthorizationCodeTokenResponseClient` để exchange authorization code thành access token.
  - Khi nhận `OAuth2AuthorizationCodeAuthenticationToken`, tạo `OAuth2AuthorizationCodeGrantRequest` từ nó.
  - Gọi token response client để lấy access token + refresh token.
  - Gọi `socialLoginService.loadUser()` với OAuth2UserRequest chứa access token (QUAN TRỌNG).
  - Tạo `OAuth2LoginAuthenticationToken` kèm user info để pass sang success handler.
- Sửa `.oauth2Login()`: thay `.authenticationManager(socialLoginService)` → `.authenticationManager(oAuth2LoginAuthenticationManager())`.

### Hành vi mới
1. OAuth2 authorization code được trả về từ provider.
2. `oAuth2LoginAuthenticationManager` intercept request.
3. Exchange authorization code thành access token.
4. Gọi `socialLoginService.loadUser()` để fetch user info từ provider + xử lý (tạo user mới nếu cần, fetch email từ GitHub nếu null...).
5. Tạo authenticated OAuth2LoginAuthenticationToken.
6. `SocialLoginSuccessHandler` nhận authentication token đã load đầy đủ user info.
7. Redirect sang frontend với HttpOnly cookies.

### Kết quả
- **Trước**: `SocialLoginSuccessHandler` được gọi trực tiếp với user info không đầy đủ → `email=null` không được fetch từ GitHub.
- **Sau**: `SocialLoginService.loadUser()` được gọi với access token → có thể fetch email từ GitHub API nếu null.
- **Compilation**: ✅ Thành công (EXIT_CODE=0).
