package com.dauducbach.clone.commons.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public enum ErrorCode {
    USER_NOT_FOUND(1000, "User not found", HttpStatus.NOT_FOUND),
    PASSWORD_INCORRECT(1001, "Incorrect password", HttpStatus.UNAUTHORIZED),
    INVALID_TOKEN(1002, "Invalid token", HttpStatus.UNAUTHORIZED),
    TOKEN_VERIFICATION_FAILED(1003, "Token verification failed", HttpStatus.UNAUTHORIZED),
    REFRESH_TOKEN_FAILED(1004, "Refresh token failed", HttpStatus.UNAUTHORIZED),
    AUTHENTICATION_FAILED(1005, "Authentication failed", HttpStatus.UNAUTHORIZED),
    EMAIL_ALREADY_LINKED(1006, "Email is already linked to another account", HttpStatus.CONFLICT),
    USERNAME_EXISTS(1007, "Username already exists", HttpStatus.CONFLICT),
    INVALID_REGISTRATION_CODE_INFO(1008, "Invalid registration(code is expired) information, please try again", HttpStatus.BAD_REQUEST),
    CODE_CREATION_FAILED(1009, "Failed to create code", HttpStatus.INTERNAL_SERVER_ERROR),
    INVALID_REGISTRATION_REQUEST_INFO(1010, "Invalid registration(request is expired) information, please try again", HttpStatus.BAD_REQUEST),
    INVALID_VERIFICATION_CODE(1011, "Incorrect verification code, please try again", HttpStatus.BAD_REQUEST),
    EMAIL_NOT_LINKED(1012, "Email is not linked to any account", HttpStatus.NOT_FOUND),
    TIMEOUT(1013, "Timeout, please re-enter your information!", HttpStatus.REQUEST_TIMEOUT),
    INVALID_VERIFICATION(1014, "Invalid verification", HttpStatus.BAD_REQUEST),
    SEND_PASSWORD_FAILED(1015, "Failed to send new password, please re-enter your information.", HttpStatus.INTERNAL_SERVER_ERROR),

    KAFKA_SEND_MESSAGE_FOR_EVENT_FAIL(3000, "Send message fail", HttpStatus.BAD_REQUEST),
    ;
    private final int code;
    private final String message;
    private final HttpStatus httpStatus;

    ErrorCode(int code, String message, HttpStatus httpStatus) {
        this.code = code;
        this.message = message;
        this.httpStatus = httpStatus;
    }
}
