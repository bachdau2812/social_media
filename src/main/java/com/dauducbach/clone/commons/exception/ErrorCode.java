package com.dauducbach.clone.commons.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public enum ErrorCode {
    USER_NOT_FOUND(1000, "User not found", HttpStatus.NOT_FOUND),
    PASSWORD_INCORRECT(1001, "Incorrect password", HttpStatus.UNAUTHORIZED),
    INVALID_TOKEN(1002, "Invalid token 4", HttpStatus.UNAUTHORIZED),
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
    ACCESS_TOKEN_EXPIRED(1016, "Access token expired, please refresh token", HttpStatus.UNAUTHORIZED),

    USER_DETAILS_NOT_FOUND(1017, "User details not found", HttpStatus.NOT_FOUND),
    USER_DETAILS_SAVE_FAILED(1018, "Save user details failed", HttpStatus.INTERNAL_SERVER_ERROR),
    USER_DETAILS_UPDATE_FAILED(1019, "Update user details failed", HttpStatus.INTERNAL_SERVER_ERROR),
    USER_DETAILS_DELETE_FAILED(1020, "Delete user details failed", HttpStatus.INTERNAL_SERVER_ERROR),
    USER_DETAILS_FETCH_FAILED(1021, "Fetch user details failed", HttpStatus.INTERNAL_SERVER_ERROR),

    USER_JOB_NOT_FOUND(1022, "User job not found", HttpStatus.NOT_FOUND),
    USER_JOB_SAVE_FAILED(1023, "Save user job failed", HttpStatus.INTERNAL_SERVER_ERROR),
    USER_JOB_UPDATE_FAILED(1024, "Update user job failed", HttpStatus.INTERNAL_SERVER_ERROR),
    USER_JOB_DELETE_FAILED(1025, "Delete user job failed", HttpStatus.INTERNAL_SERVER_ERROR),
    USER_JOB_FETCH_FAILED(1026, "Fetch user job failed", HttpStatus.INTERNAL_SERVER_ERROR),

    USER_PHONE_NOT_FOUND(1027, "User phone not found", HttpStatus.NOT_FOUND),
    USER_PHONE_SAVE_FAILED(1028, "Save user phone failed", HttpStatus.INTERNAL_SERVER_ERROR),
    USER_PHONE_DELETE_FAILED(1029, "Delete user phone failed", HttpStatus.INTERNAL_SERVER_ERROR),
    USER_PHONE_FETCH_FAILED(1030, "Fetch user phone failed", HttpStatus.INTERNAL_SERVER_ERROR),

    USER_HIGH_SCHOOL_NOT_FOUND(1031, "User high school not found", HttpStatus.NOT_FOUND),
    USER_HIGH_SCHOOL_SAVE_FAILED(1032, "Save user high school failed", HttpStatus.INTERNAL_SERVER_ERROR),
    USER_HIGH_SCHOOL_DELETE_FAILED(1033, "Delete user high school failed", HttpStatus.INTERNAL_SERVER_ERROR),
    USER_HIGH_SCHOOL_FETCH_FAILED(1034, "Fetch user high school failed", HttpStatus.INTERNAL_SERVER_ERROR),

    USER_UNIVERSITY_NOT_FOUND(1035, "User university not found", HttpStatus.NOT_FOUND),
    USER_UNIVERSITY_SAVE_FAILED(1036, "Save user university failed", HttpStatus.INTERNAL_SERVER_ERROR),
    USER_UNIVERSITY_DELETE_FAILED(1037, "Delete user university failed", HttpStatus.INTERNAL_SERVER_ERROR),
    USER_UNIVERSITY_FETCH_FAILED(1038, "Fetch user university failed", HttpStatus.INTERNAL_SERVER_ERROR),

    USER_SOCIAL_MEDIA_NOT_FOUND(1039, "User social media not found", HttpStatus.NOT_FOUND),
    USER_SOCIAL_MEDIA_SAVE_FAILED(1040, "Save user social media failed", HttpStatus.INTERNAL_SERVER_ERROR),
    USER_SOCIAL_MEDIA_DELETE_FAILED(1041, "Delete user social media failed", HttpStatus.INTERNAL_SERVER_ERROR),
    USER_SOCIAL_MEDIA_FETCH_FAILED(1042, "Fetch user social media failed", HttpStatus.INTERNAL_SERVER_ERROR),

    FOLLOW_RELATIONSHIP_NOT_FOUND(1043, "Follow relationship not found", HttpStatus.NOT_FOUND),
    FOLLOW_RELATIONSHIP_FETCH_FAILED(1044, "Fetch follow relationship failed", HttpStatus.INTERNAL_SERVER_ERROR),
    CANNOT_FOLLOW_SELF(1045, "Cannot follow yourself", HttpStatus.BAD_REQUEST),
    ALREADY_FOLLOWING_USER(1046, "Already following this user", HttpStatus.CONFLICT),
    NOT_FOLLOWING_USER(1047, "Not following this user", HttpStatus.BAD_REQUEST),
    FOLLOW_SAVE_FAILED(1048, "Follow user failed", HttpStatus.INTERNAL_SERVER_ERROR),
    UNFOLLOW_FAILED(1049, "Unfollow user failed", HttpStatus.INTERNAL_SERVER_ERROR),
    FOLLOWERS_FETCH_FAILED(1050, "Fetch followers failed", HttpStatus.INTERNAL_SERVER_ERROR),
    FOLLOWING_FETCH_FAILED(1051, "Fetch following failed", HttpStatus.INTERNAL_SERVER_ERROR),
    FOLLOW_COUNT_FAILED(1052, "Fetch follower counts failed", HttpStatus.INTERNAL_SERVER_ERROR),
    FOLLOW_STATUS_CHECK_FAILED(1053, "Check following status failed", HttpStatus.INTERNAL_SERVER_ERROR),
    PROFILE_MEDIA_INVALID(1054, "Profile media request is invalid", HttpStatus.BAD_REQUEST),
    PROFILE_MEDIA_PROCESS_FAILED(1055, "Process profile media failed", HttpStatus.INTERNAL_SERVER_ERROR),
    MUSIC_NOT_FOUND(1056, "Music not found", HttpStatus.NOT_FOUND),
    MUSIC_SAVE_FAILED(1057, "Save profile music failed", HttpStatus.INTERNAL_SERVER_ERROR),
    STORY_NOT_FOUND(1058, "Story not found", HttpStatus.NOT_FOUND),
    STORY_SAVE_FAILED(1059, "Save story failed", HttpStatus.INTERNAL_SERVER_ERROR),
    MUSIC_FETCH_FAILED(1060, "Fetch music failed", HttpStatus.INTERNAL_SERVER_ERROR),
    MUSIC_IMPORT_FAILED(1061, "Import music failed", HttpStatus.BAD_REQUEST),
    MUSIC_REQUEST_INVALID(1062, "Music request is invalid", HttpStatus.BAD_REQUEST),
    USER_SEARCH_FAILED(1063, "Search users failed", HttpStatus.INTERNAL_SERVER_ERROR),
    SEARCH_REQUEST_INVALID(1064, "Search request is invalid", HttpStatus.BAD_REQUEST),
    SEARCH_SUGGESTION_FAILED(1065, "Fetch search suggestions failed", HttpStatus.INTERNAL_SERVER_ERROR),
    SEARCH_HISTORY_UPDATE_FAILED(1066, "Update search history failed", HttpStatus.INTERNAL_SERVER_ERROR),

    POST_NOT_FOUND(1100, "Post not found", HttpStatus.NOT_FOUND),
    POST_CREATE_FAILED(1101, "Create post failed", HttpStatus.INTERNAL_SERVER_ERROR),
    POST_UPDATE_FAILED(1102, "Update post failed", HttpStatus.INTERNAL_SERVER_ERROR),
    POST_DELETE_FAILED(1103, "Delete post failed", HttpStatus.INTERNAL_SERVER_ERROR),
    POST_FETCH_FAILED(1104, "Fetch post failed", HttpStatus.INTERNAL_SERVER_ERROR),
    POST_LIST_FETCH_FAILED(1105, "Fetch posts failed", HttpStatus.INTERNAL_SERVER_ERROR),
    POST_CONTENT_INVALID(1106, "Post content is invalid", HttpStatus.BAD_REQUEST),
    POST_MEDIA_UPLOAD_FAILED(1107, "Post media upload failed", HttpStatus.BAD_REQUEST),
    MEDIA_SIGNATURE_FAILED(1108, "Generate media signature failed", HttpStatus.INTERNAL_SERVER_ERROR),
    MEDIA_SCAN_FAILED(1109, "Media scan failed", HttpStatus.INTERNAL_SERVER_ERROR),
    CLOUDINARY_API_ERROR(1110, "Cloudinary API error", HttpStatus.INTERNAL_SERVER_ERROR),
    MEDIA_SAVE_FAILED(1111, "Save media metadata failed", HttpStatus.INTERNAL_SERVER_ERROR),
    MEDIA_NOT_FOUND(1112, "Media not found", HttpStatus.NOT_FOUND),
    MEDIA_FETCH_FAILED(1113, "Fetch media failed", HttpStatus.INTERNAL_SERVER_ERROR),
    POST_NOTIFICATION_MUTE_FAILED(1114, "Mute post notification failed", HttpStatus.INTERNAL_SERVER_ERROR),
    POST_SEARCH_FAILED(1115, "Search posts failed", HttpStatus.INTERNAL_SERVER_ERROR),

    COMMENT_NOT_FOUND(1120, "Comment not found", HttpStatus.NOT_FOUND),
    COMMENT_CREATE_FAILED(1121, "Create comment failed", HttpStatus.INTERNAL_SERVER_ERROR),
    COMMENT_DELETE_FAILED(1122, "Delete comment failed", HttpStatus.INTERNAL_SERVER_ERROR),
    COMMENT_FETCH_FAILED(1123, "Fetch comment failed", HttpStatus.INTERNAL_SERVER_ERROR),
    COMMENT_UPDATE_FAILED(1124, "Update comment failed", HttpStatus.INTERNAL_SERVER_ERROR),
    COMMENT_CONTENT_INVALID(1125, "Comment content is invalid", HttpStatus.BAD_REQUEST),
    COMMENT_FORBIDDEN(1126, "User is not allowed to modify this comment", HttpStatus.FORBIDDEN),
    GET_VECTOR_EMBEDDING_FAILED(1127, "Get embedding failed", HttpStatus.INTERNAL_SERVER_ERROR),

    INVALID_TARGET_TYPE(1130, "Target type must be POST or COMMENT", HttpStatus.BAD_REQUEST),
    TARGET_NOT_FOUND(1131, "Target not found", HttpStatus.NOT_FOUND),
    ALREADY_LIKED(1132, "Target is already liked by this user", HttpStatus.CONFLICT),
    LIKE_NOT_FOUND(1133, "Like not found", HttpStatus.NOT_FOUND),
    LIKE_CREATE_FAILED(1134, "Create like failed", HttpStatus.INTERNAL_SERVER_ERROR),
    LIKE_DELETE_FAILED(1135, "Delete like failed", HttpStatus.INTERNAL_SERVER_ERROR),
    LIKE_FETCH_FAILED(1136, "Fetch like failed", HttpStatus.INTERNAL_SERVER_ERROR),

    KAFKA_SEND_MESSAGE_FOR_EVENT_FAIL(3000, "Send message fail", HttpStatus.BAD_REQUEST),

    REFRESH_TOKEN_INVALID(4000, "User haven't had valid refresh token", HttpStatus.FORBIDDEN),
    LOGOUT_FAILED(4001, "Logout failed", HttpStatus.INTERNAL_SERVER_ERROR),
    INTROSPECTION_FAILED(4002, "Introspection failed", HttpStatus.INTERNAL_SERVER_ERROR),
    LOAD_USER_FROM_SOCIAL_MEDIA_FAIL(4003, "Load user from social media failed", HttpStatus.FORBIDDEN),
    MISSING_USER_INFO_FROM_SOCIAL_MEDIA(4004, "Missing user info from social media", HttpStatus.BAD_REQUEST),

    SEND_EMAIL_FAILED(5000, "Send email failed", HttpStatus.INTERNAL_SERVER_ERROR),
    SEND_PUSH_NOTIFICATION_FAILED(5001, "Send push notification failed", HttpStatus.INTERNAL_SERVER_ERROR),
    NOTIFICATION_TYPE_NOT_SUPPORTED(5002, "Notification type not supported", HttpStatus.BAD_REQUEST),
    PUSH_TOKEN_SAVE_FAILED(5003, "Save push token failed", HttpStatus.INTERNAL_SERVER_ERROR),
    PUSH_TOKEN_INVALID(5004, "Push token is invalid", HttpStatus.BAD_REQUEST),
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
