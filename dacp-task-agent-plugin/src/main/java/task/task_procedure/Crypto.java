package task.task_procedure;

/**
 * 算法包
 * @author MeiKefu
 * @date 2015-10-25
 */
public interface Crypto {
	
	public static String DEFAULT_AES_IV_KEY = "7b51fd7053196308";
	
	public static String DEFAULT_SECRET_KEY = "b6fa92796c6431c5";

	public static String DEFAULT_RSA_PUB_KEY = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCSq1P5pwKjQ3sRCm1Ku2eSEIlzeJ+1WjsvUD79oUHCWEk6s4p+/9v40DodSXbTBR+zaQrM18068fj8dnHUNugFvHY9VmGSWdA4t3eAv/gqJgqyH3hLTCkc5tFQb41AvlOAh+UkPGdeLJzADgMsbOLW+oprgJTlCGN4qyGt4PMklwIDAQAB";

	public static String DEFAULT_RSA_PRV_KEY = "MIICdQIBADANBgkqhkiG9w0BAQEFAASCAl8wggJbAgEAAoGBAJKrU/mnAqNDexEKbUq7Z5IQiXN4n7VaOy9QPv2hQcJYSTqzin7/2/jQOh1JdtMFH7NpCszXzTrx+Px2cdQ26AW8dj1WYZJZ0Di3d4C/+ComCrIfeEtMKRzm0VBvjUC+U4CH5SQ8Z14snMAOAyxs4tb6imuAlOUIY3irIa3g8ySXAgMBAAECgYBbD3YOrQstNCtHNDMQz/OIsy2zfHOj08alJGxHagP61nd4s5nMKax3+UfrgP3+G9NHv3LOJTmtXCgbDqqzd/drD5HfmZ8oijt2v5+JpaA5v0NE8bJ+YuGnZHsvRxnHSg3af41WuJFi8ypQbvwoM0wvK2m9mhwv3a+SZuoIio0eAQJBAMfm9zK42wnEoJTfWVDJQ8VTsvNzPykJp/hVehR3Kuzb/tYeClEa88Nu7a27PsirTZyQtKZz8mgzpQtl4HLRHQECQQC71Bc5zp2ljGi+/WQSbTJ/QrUTX2ajz8JOHn8LjBNw6Ig/ba7UdyuZG2CT2yN7e4b6kxkFcQvuc7AF20bPlgmXAkA0rJqUpfsprsM1l0jcrbwK4gw0dDi/Yoj9OeDKRcBs22diGlZTmsuCwtckQUPA8sqnp8XCWqkUZg9mWnjzKEEBAkA2Um6tL050tJkOs6YflilGmPpZsfuzYMDnavZ/VnHA89+DyKsJk7WuQDa8TGKEsgMlsSlJY5p5ykecSgC+xLvdAkBJj/C6py0yWUSQvXhBTro6iDmet2RmK345kmsM190dub4Q5t9gKRNcCSjIUAQiennpu1viqxP1cXsLUrVUon61";

	/**
	 * 加密无私钥
	 * @param message 明文
	 * @return
	 */
	public String encrypt(String message);
	
	/**
	 * 加密带私钥
	 * @param message 明文
	 * @param key 私钥
	 * @return
	 */
	public String encrypt(String message,String key);
	
	/**
	 * 解密无私钥
	 * @param encryptedData 明文
	 * @return
	 */
	public String decrypt(String encryptedData);
	
	/**
	 * 解密带私钥
	 * @param encryptedData 明文
	 * @param key 私钥
	 * @return
	 */
	public String decrypt(String encryptedData, String key);
}
