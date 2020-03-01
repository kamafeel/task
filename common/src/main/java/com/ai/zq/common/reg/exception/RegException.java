

package com.ai.zq.common.reg.exception;

/**
 * ZK注册中心异常.
 * 
 * @author zhangqi
 */
public final class RegException extends RuntimeException {
    
    private static final long serialVersionUID = -6417179023552012152L;
    
    public RegException(final String errorMessage, final Object... args) {
        super(String.format(errorMessage, args));
    }
    
    public RegException(final Exception cause) {
        super(cause);
    }
}
