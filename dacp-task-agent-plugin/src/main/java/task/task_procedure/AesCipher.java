package task.task_procedure;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * AES算法包
 * 
 * @author MeiKefu
 * @date 2015-10-25
 */
public class AesCipher {
	/*
	 *
        算法/模式/填充                16字节加密后数据长度        不满16字节加密后长度
        AES/CBC/NoPadding             16                          不支持
        AES/CBC/PKCS5Padding          32                          16
        AES/CBC/ISO10126Padding       32                          16
        AES/CFB/NoPadding             16                          原始数据长度
        AES/CFB/PKCS5Padding          32                          16
        AES/CFB/ISO10126Padding       32                          16
        AES/ECB/NoPadding             16                          不支持
        AES/ECB/PKCS5Padding          32                          16
        AES/ECB/ISO10126Padding       32                          16
        AES/OFB/NoPadding             16                          原始数据长度
        AES/OFB/PKCS5Padding          32                          16
        AES/OFB/ISO10126Padding       32                          16
        AES/PCBC/NoPadding            16                          不支持
        AES/PCBC/PKCS5Padding         32                          16
        AES/PCBC/ISO10126Padding      32                          16
	 * 
	 */
	private static final String ALGORITHM = "AES/CBC/PKCS5Padding";
	
	private static byte[] ivValue = null;
	static {
		try {
			ivValue = Crypto.DEFAULT_AES_IV_KEY.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	private static final IvParameterSpec IV_SPEC = new IvParameterSpec(ivValue);
	
	/**
	 * 采用默认的key加密数据
	 * @param message
	 * @return
	 */
	public static String encrypt(String message){
		return encrypt(message, Crypto.DEFAULT_SECRET_KEY);
	}

	/**
	 * 通过key值加密数据
	 * @param message
	 * @param key
	 * @return 加密后的数据
	 */
	public static String encrypt(String message,String key){
		try {
			return encrypt(message,new SecretKeySpec(CipherHelper.fillChar(key, 16).getBytes("UTF-8"), "AES"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return message;
	}

    public static String encrypt(String data,String key,String iv){
        try {
            return encrypt(data,ALGORITHM,new SecretKeySpec(CipherHelper.fillChar(key, 16).getBytes("UTF-8"), "AES"),new IvParameterSpec(iv.getBytes("UTF-8")));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return data;

	}

	public static String encrypt(String data,SecretKeySpec keySpec){
		return encrypt(data,ALGORITHM,keySpec,IV_SPEC);
	}
	
	/**
	 * 根据算法模式，key，iv信息加密数据
	 * @param message 信息
	 * @param algorithm 算法
	 * @param keySpec  key
	 * @param ivSpec iv信息
	 * @return
	 */
	public static String encrypt(String message,String algorithm,SecretKeySpec keySpec,IvParameterSpec ivSpec){
		try {
			Cipher c = Cipher.getInstance(algorithm);
			if(ivSpec!=null){
				c.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);
			}else{
				c.init(Cipher.ENCRYPT_MODE, keySpec);
			}
			
			byte[] encVal = c.doFinal(message.getBytes("UTF-8"));
			String encryptedValue = Base64.encodeBase64String(encVal);
			return encryptedValue;
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (InvalidAlgorithmParameterException e) {
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			e.printStackTrace();
		} catch (BadPaddingException e) {
			e.printStackTrace();
		}
		return message;
	}

	/**
	 * 采用默认key解密数据
	 * @param encryptedData 待解密的数据
	 * @return 解密后的结果
	 */
	public static String decrypt(String encryptedData) {
		return decrypt(encryptedData, Crypto.DEFAULT_SECRET_KEY);
	}

	/**
	 * 采用指定key解密数据
	 * @param encryptedData 待解密的数据
	 * @param key 密钥
	 * @return 解密后的结果
	 */
	public static String decrypt(String encryptedData, String key) {
		try {
			return decrypt(encryptedData,new SecretKeySpec(CipherHelper.fillChar(key, 16).getBytes("UTF-8"), "AES"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return encryptedData;
	}

    public static String decrypt(String encryptedData, String key ,String iv) {
        try {
            return decrypt(encryptedData,ALGORITHM,new SecretKeySpec(CipherHelper.fillChar(key, 16).getBytes("UTF-8"), "AES"),new IvParameterSpec(iv.getBytes("UTF-8")));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return encryptedData;
    }

	public static String decrypt(String data,SecretKeySpec keySpec){
		return decrypt(data,ALGORITHM,keySpec,IV_SPEC);
	}

	/**
	 * 根据算法模式，key，iv信息解密数据
	 * @param encryptedData 待解密的内容
	 * @param algorithm 算法
	 * @param keySpec  key
	 * @param ivSpec iv信息
	 * @return
	 */
	public static String decrypt(String encryptedData, String algorithm,SecretKeySpec keySpec,IvParameterSpec ivSpec) {
		if(encryptedData==null || encryptedData.length()==0){
			return "";
		}
		try {
			Cipher c = Cipher.getInstance(algorithm);
			if(ivSpec!=null){
				c.init(Cipher.DECRYPT_MODE, keySpec,ivSpec);
			}else{
				c.init(Cipher.DECRYPT_MODE, keySpec);
			}
			byte[] decordedValue = Base64.decodeBase64(encryptedData);
			byte[] decValue = c.doFinal(decordedValue);
			String decryptedValue = new String(decValue,"utf-8");
			return decryptedValue;
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (InvalidAlgorithmParameterException e) {
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			e.printStackTrace();
		} catch (BadPaddingException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return encryptedData;
	}

}
