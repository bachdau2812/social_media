package com.dauducbach.clone.commons.response;

import lombok.*;
import lombok.experimental.FieldDefaults;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@FieldDefaults(level = AccessLevel.PRIVATE)

public class ApiResponse<T> {
    @Builder.Default
    int code = 2000;
    String message;

    String traceId;

    T result;
}
