

package com.ai.zq.common.util.env;

import java.io.IOException;

/**
 * 网络主机异常.
 * 
 * @author zhangqi
 */
public final class HostException extends RuntimeException {
    
    private static final long serialVersionUID = 3589264847881174997L;
    
    public HostException(final IOException cause) {
        super(cause);
    }
}
