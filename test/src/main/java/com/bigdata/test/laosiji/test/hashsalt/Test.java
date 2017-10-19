package com.bigdata.test.laosiji.test.hashsalt;

import java.security.NoSuchAlgorithmException;

public class Test {
    public static void main(String[] args) throws NoSuchAlgorithmException {
        String salt = CryptoUtils.getSalt();
        String password = "admin123";
        String hashPassword = CryptoUtils.getHash(password, salt);
        System.out.println("hashPassword:" + hashPassword);
        System.out.println("salt:" + salt);
        System.out.println("password:" + password);
        // verify
        boolean result = CryptoUtils.verify(hashPassword, password, salt);
        System.out.println("Verify:" + result);

    }
}
