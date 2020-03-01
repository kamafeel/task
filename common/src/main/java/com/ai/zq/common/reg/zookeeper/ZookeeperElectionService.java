

package com.ai.zq.common.reg.zookeeper;

import com.ai.zq.common.exception.JobSystemException;
import com.ai.zq.common.reg.base.ElectionCandidate;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import java.util.concurrent.CountDownLatch;

/**
 * 使用{@link LeaderSelector}实现选举服务.
 * @author zhangqi
 */
@Slf4j
public final class ZookeeperElectionService {
    
    private final CountDownLatch leaderLatch = new CountDownLatch(1);
    
    private final LeaderSelector leaderSelector;
    
    public ZookeeperElectionService(final String identity, final CuratorFramework client, final String electionPath, final ElectionCandidate electionCandidate) {
        leaderSelector = new LeaderSelector(client, electionPath, new LeaderSelectorListenerAdapter() {
            
            @Override
            public void takeLeadership(final CuratorFramework client) throws Exception {
                log.info("dacp-task-server:{} has leadership", identity);
                try {
                    electionCandidate.startLeadership();
                    leaderLatch.await();
                    log.warn("dacp-task-server:{} lost leadership.", identity);
                    electionCandidate.stopLeadership();
                } catch (final JobSystemException exception) {
                    log.error("dacp-task:Starting error", exception);
                    System.exit(1);  
                }
            }
        });
        leaderSelector.autoRequeue();
        leaderSelector.setId(identity);
    }
    
    /**
     * 开始选举.
     */
    public void start() {
        log.debug("dacp-task : {} start to elect leadership", leaderSelector.getId());
        leaderSelector.start();
    }
    
    /**
     * 停止选举.
     */
    public void stop() {
        log.info("dacp-task : stop leadership election");
        leaderLatch.countDown();
        try {
            leaderSelector.close();
            // CHECKSTYLE:OFF
        } catch (final Exception ignored) {
        }
        // CHECKSTYLE:ON
    }
}
