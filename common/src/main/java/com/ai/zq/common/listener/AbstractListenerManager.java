package com.ai.zq.common.listener;

import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;

import com.ai.zq.common.reg.base.CoordinatorRegistryCenter;

/**
 * ZK注册中心的对象监听器管理者的抽象类.
 * 
 * @author zhangqi
 */
public abstract class AbstractListenerManager {
    
    public final CoordinatorRegistryCenter regCenter;
    
    protected AbstractListenerManager(final CoordinatorRegistryCenter regCenter) {
    	this.regCenter = regCenter;
    }

    /**
     * 开启监听器.
     */
    public abstract void start();
    
    protected void addDataListener(final TreeCacheListener listener,String path) {
    	TreeCache cache = (TreeCache) regCenter.getRawCache(path);
        cache.getListenable().addListener(listener);
    }
}
