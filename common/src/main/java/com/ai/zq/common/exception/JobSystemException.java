

package com.ai.zq.common.exception;

/**
 * ZK系统异常.
 * 
 * @author zhangqi
 */
public final class JobSystemException extends RuntimeException {
    
    private static final long serialVersionUID = 5018901344199973515L;
    
    public JobSystemException(final String errorMessage, final Object... args) {
        super(String.format(errorMessage, args));
    }
    
    public JobSystemException(final Throwable cause) {
        super(cause);
    }
}
