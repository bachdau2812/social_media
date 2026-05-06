package com.dauducbach.clone.modules.auth.service;

import java.security.SecureRandom;

public class RandomCode {
    private static final String CODE_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            + "abcdefghijklmnopqrstuvwxyz"
            + "0123456789";

    private static final String PASSWORD_CHARACTERS =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                    "abcdefghijklmnopqrstuvwxyz" +
                    "0123456789" +
                    "!@#$%^&*()-_=+[]{};:,.<>?/";

    private static final SecureRandom random = new SecureRandom();

    public static String generateRandomCode(int length) {
        StringBuilder sb = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            int index = random.nextInt(CODE_CHARACTERS.length());
            sb.append(CODE_CHARACTERS.charAt(index));
        }

        return sb.toString();
    }

    public static String generateRandomPassword(int length) {
        StringBuilder sb = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            int index = random.nextInt(PASSWORD_CHARACTERS.length());
            sb.append(PASSWORD_CHARACTERS.charAt(index));
        }

        return sb.toString();
    }
}
