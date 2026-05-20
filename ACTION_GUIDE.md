# ACTION_GUIDE

> Tài liệu này là hướng dẫn thao tác/test luồng auth hiện tại. Khi có thay đổi liên quan đến auth/security/cookie/refresh-token hoặc cách test, hãy **append thêm một mục mới ở cuối file** này để cập nhật.

## 1) Tổng quan luồng hiện tại

Project đang chạy theo flow auth reactive với cookie HttpOnly:

- `accessToken` được lưu vào cookie HttpOnly
- `refreshToken` được lưu vào cookie HttpOnly
- `deviceInfo` được lưu vào cookie HttpOnly
- FE phải gửi request với `credentials`
- Backend hỗ trợ:
  - `POST /auth/login`
  - `POST /auth/refresh-token`
  - `POST /auth/logout`
  - `POST /auth/introspect`

### Base URL thực tế

Vì `application.yaml` đang có:

```yaml
server:
  port: 8888
spring:
  webflux:
    base-path: /app
```

nên các API auth sẽ nằm dưới:

```text
http://localhost:8888/app
```

Ví dụ:

```text
POST http://localhost:8888/app/auth/login
POST http://localhost:8888/app/auth/refresh-token
POST http://localhost:8888/app/auth/logout
POST http://localhost:8888/app/auth/introspect
```

---

## 2) Cookie names đang dùng

Các cookie auth hiện tại:

- `accessToken`
- `refreshToken`
- `deviceInfo`

### Lưu ý

- Cookie đều được set theo kiểu `HttpOnly`
- FE **không đọc trực tiếp** được các cookie này bằng JavaScript
- FE chỉ cần gửi request kèm credentials để browser tự attach cookie

---

## 3) Test bằng Postman

### 3.1. Login

#### Request

```http
POST http://localhost:8888/app/auth/login
Content-Type: application/json
```

#### Body

```json
{
  "username": "your_username",
  "password": "your_password",
  "deviceInfo": "postman-windows"
}
```

#### Kết quả mong đợi

- Response status: `200`
- Response body có dạng `ApiResponse`
- Header response có set cookie:
  - `accessToken`
  - `refreshToken`
  - `deviceInfo`

### 3.2. Kiểm tra cookie trong Postman

Sau khi login thành công:

- mở tab **Cookies** của domain `localhost`
- kiểm tra đã có 3 cookie:
  - `accessToken`
  - `refreshToken`
  - `deviceInfo`

Nếu chưa thấy cookie:

- kiểm tra request login có đúng URL chưa
- kiểm tra response có status `200` không
- kiểm tra Postman có đang lưu cookie jar không

### 3.3. Refresh token

#### Cách 1: gửi body

```http
POST http://localhost:8888/app/auth/refresh-token
Content-Type: application/json
```

```json
{
  "refreshToken": "<refreshToken>",
  "deviceInfo": "postman-windows"
}
```

#### Cách 2: không gửi body, dùng cookie

Nếu Postman đã lưu cookie sau login, có thể gọi:

```http
POST http://localhost:8888/app/auth/refresh-token
```

Backend sẽ đọc `refreshToken` và `deviceInfo` từ cookie.

#### Kết quả mong đợi khi refresh thành công

- Response status: `200`
- Response body trả `ApiResponse`
- Cookie mới được set lại:
  - `accessToken`
  - `refreshToken`
  - `deviceInfo`

### 3.4. Logout

#### Cách 1: gửi body

```http
POST http://localhost:8888/app/auth/logout
Content-Type: application/json
```

```json
{
  "accessToken": "<accessToken>",
  "refreshToken": "<refreshToken>",
  "deviceInfo": "postman-windows"
}
```

#### Cách 2: không gửi body, dùng cookie

Nếu cookie đã có sẵn:

```http
POST http://localhost:8888/app/auth/logout
```

Backend sẽ đọc từ cookie rồi logout.

#### Kết quả mong đợi khi logout thành công

- Response status: `200`
- Response body: `Logout successful`
- Cookie cũ bị clear:
  - `accessToken`
  - `refreshToken`
  - `deviceInfo`

### 3.5. Introspect

```http
POST http://localhost:8888/app/auth/introspect
Content-Type: application/json
```

```json
{
  "accessToken": "<accessToken>"
}
```

---

## 4) Luồng test khi accessToken hết hạn

### Mục tiêu

Khi `accessToken` hết hạn:

- backend sẽ trả `401`
- `ApiResponse.code = 1016`
- `message = Access token expired, please refresh token`
- frontend phải tự gọi `POST /auth/refresh-token`

### Cách test thủ công

1. Login lấy cookie
2. Chờ access token hết hạn
3. Gọi API cần auth (hoặc gọi `POST /auth/introspect` với token hết hạn)
4. Kiểm tra response `401`
5. Gọi `POST /auth/refresh-token`
6. Kiểm tra cookie được set lại
7. Gọi lại API ban đầu

---

## 5) Test bằng Frontend

### 5.1. Nguyên tắc bắt buộc

Frontend phải gửi request với `credentials` thì browser mới tự gửi/nhận cookie:

- `fetch`: dùng `credentials: 'include'`
- `axios`: dùng `withCredentials: true`

### 5.2. Ví dụ với fetch

```js
await fetch('http://localhost:8888/app/auth/login', {
  method: 'POST',
  credentials: 'include',
  headers: {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    username: 'your_username',
    password: 'your_password',
    deviceInfo: 'chrome-frontend'
  })
});
```

### 5.3. Ví dụ với axios

```js
import axios from 'axios';

const api = axios.create({
  baseURL: 'http://localhost:8888/app',
  withCredentials: true
});
```

### 5.4. Refresh token tự động khi gặp 401

#### Ý tưởng

- Gọi API bình thường
- Nếu response trả `401` và code báo hết hạn token:
  - gọi `POST /auth/refresh-token`
  - backend sẽ đọc `refreshToken` và `deviceInfo` từ cookie
  - backend set lại cookie mới
  - retry lại request cũ

### 5.5. Ví dụ interceptor với axios

```js
import axios from 'axios';

const api = axios.create({
  baseURL: 'http://localhost:8888/app',
  withCredentials: true,
});

api.interceptors.response.use(
  (response) => response,
  async (error) => {
    const originalRequest = error.config;

    if (error.response?.status === 401 && !originalRequest._retry) {
      originalRequest._retry = true;

      try {
        await api.post('/auth/refresh-token');
        return api(originalRequest);
      } catch (refreshError) {
        // refresh thất bại -> chuyển về login
        window.location.href = '/login';
        return Promise.reject(refreshError);
      }
    }

    return Promise.reject(error);
  }
);
```

### 5.6. Logout từ Frontend

```js
await api.post('/auth/logout');
```

Kết quả mong đợi:

- cookie bị clear
- FE clear state local nếu có
- redirect về trang login

---

## 6) Khi refresh token bị lỗi

Nếu refresh token không hợp lệ/hết hạn/thiếu cookie:

- backend sẽ revoke các refresh token active của user
- response trả `AppException`
- frontend nên:
  - xoá state đăng nhập cục bộ
  - redirect về login
  - không retry request nữa

---

## 7) Các tình huống cần nhớ khi test

### Postman

- Nếu muốn dùng cookie tự động, phải giữ cùng 1 domain/host
- Không nên đổi qua lại giữa `localhost` và `127.0.0.1`
- Nên giữ đúng `http://localhost:8888/app`

### Frontend

- Phải bật `credentials`
- Nếu FE chạy ở `localhost:5173` hoặc `localhost:5000` thì backend CORS đã cho phép credentials
- Không thể đọc cookie HttpOnly bằng JS, đây là chủ đích bảo mật

---

## 8) Các endpoint auth hay dùng nhất

- `POST /app/auth/login`
- `POST /app/auth/refresh-token`
- `POST /app/auth/logout`
- `POST /app/auth/introspect`

---

## 9) Ghi chú cập nhật lần sau

Khi thay đổi thêm bất kỳ thứ gì liên quan tới:

- auth
- refresh token
- logout
- cookie
- CORS
- social login
- notification auth flow

hãy append thêm một section mới ở cuối file này.

