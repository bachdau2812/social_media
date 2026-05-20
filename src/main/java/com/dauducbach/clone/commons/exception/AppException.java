package com.dauducbach.clone.commons.exception;

import lombok.Getter;

@Getter
public class AppException extends RuntimeException {
    private final ErrorCode errorCode;
    private final String detailMessage;

    public AppException(ErrorCode errorCode) {
        this(errorCode, errorCode.getMessage(), null);
    }

    public AppException(ErrorCode errorCode, String detailMessage) {
        this(errorCode, detailMessage, null);
    }

    public AppException(ErrorCode errorCode, String detailMessage, Throwable cause) {
        super(detailMessage, cause);
        this.errorCode = errorCode;
        this.detailMessage = detailMessage;
    }
}
