

package com.ai.zq.common.util.digest;

import com.ai.zq.common.exception.JobSystemException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * 字符串加密工具类.
 *
 * @author zhangqi
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Encryption {
    
    private static final String MD5 = "MD5";
    
    /**
     * 采用MD5算法加密字符串.
     * 
     * @param str 需要加密的字符串
     * @return 加密后的字符串
     */
    public static String md5(final String str) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(MD5);
            messageDigest.update(str.getBytes());
            return new BigInteger(1, messageDigest.digest()).toString(16);
        } catch (final NoSuchAlgorithmException ex) {
            throw new JobSystemException(ex);
        }
    }
}
