package com.asiainfo.dacp.scheduler.service;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.asiainfo.dacp.dp.server.scheduler.bean.TaskLog;
import com.asiainfo.dacp.scheduler.event.CheckRunModelHandler;
import com.asiainfo.dacp.scheduler.event.EventFinishHandler;
import com.asiainfo.dacp.scheduler.event.FailHandler;
import com.asiainfo.dacp.scheduler.event.MQStuckHandler;
import com.asiainfo.dacp.scheduler.event.RunTimeOutHandler;
import com.asiainfo.dacp.scheduler.event.Send2MQHandler;
import com.asiainfo.dacp.scheduler.event.SucTriggerHandler;
import com.asiainfo.dacp.scheduler.event.TaskLogEvent;
import com.asiainfo.dacp.scheduler.event.TaskLogEventFactory;
import com.asiainfo.dacp.scheduler.event.broadcast.CheckDpendHandler_1;
import com.asiainfo.dacp.scheduler.event.broadcast.CheckDpendHandler_10;
import com.asiainfo.dacp.scheduler.event.broadcast.CheckDpendHandler_2;
import com.asiainfo.dacp.scheduler.event.broadcast.CheckDpendHandler_3;
import com.asiainfo.dacp.scheduler.event.broadcast.CheckDpendHandler_4;
import com.asiainfo.dacp.scheduler.event.broadcast.CheckDpendHandler_5;
import com.asiainfo.dacp.scheduler.event.broadcast.CheckDpendHandler_6;
import com.asiainfo.dacp.scheduler.event.broadcast.CheckDpendHandler_7;
import com.asiainfo.dacp.scheduler.event.broadcast.CheckDpendHandler_8;
import com.asiainfo.dacp.scheduler.event.broadcast.CheckDpendHandler_9;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * 业务事件处理服务
 * @author zhangqi
 *
 */
@Service
public class TaskDisruptorService {
	
	@Value("${disruptor.bufferSize}")
	private int ringBufferSize;
	@Value("${rule.sucTask.personality}")
	private boolean isPersonality;

	public RingBuffer<TaskLogEvent> checkDpendRB;
	public RingBuffer<TaskLogEvent> checkRunModelRB;
	public RingBuffer<TaskLogEvent> send2MQRB;
	public RingBuffer<TaskLogEvent> sucTriggerRB;
	public RingBuffer<TaskLogEvent> failRB;
	public RingBuffer<TaskLogEvent> mQStuckRB;
	public RingBuffer<TaskLogEvent> runTimeOutRB;
	
	@Autowired
	private EventFinishHandler eventFinishHandler;	
	@Autowired
	private CheckRunModelHandler checkRunModelHandler;
	@Autowired
	private SucTriggerHandler sucTriggerHandler;
	@Autowired
	private Send2MQHandler send2MQHandler;
	@Autowired
	private FailHandler failHandler;	
	@Autowired
	private MQStuckHandler mQStuckHandler;
	@Autowired
	private RunTimeOutHandler runTimeOutHandler;
	
	@Autowired
	private CheckDpendHandler_1 cdh1;
	@Autowired
	private CheckDpendHandler_2 cdh2;
	@Autowired
	private CheckDpendHandler_3 cdh3;
	@Autowired
	private CheckDpendHandler_4 cdh4;
	@Autowired
	private CheckDpendHandler_5 cdh5;
	@Autowired
	private CheckDpendHandler_6 cdh6;
	@Autowired
	private CheckDpendHandler_7 cdh7;
	@Autowired
	private CheckDpendHandler_8 cdh8;
	@Autowired
	private CheckDpendHandler_9 cdh9;
	@Autowired
	private CheckDpendHandler_10 cdh10;
	
	@Autowired
	private EventHandler<TaskLogEvent> personalityEvent;
	
	public void init() {
		this.initCheckDpendDisruptor();
		this.initCheckRunModelDisruptor();
		this.initSend2MQDisruptor();
		this.initSucTriggerDisruptor();
		this.initFailHandlerDisruptor();
		this.initMQStuckHandlerDisruptor();
		this.initRunTimeOutHandlerDisruptor();
	}
	
	private void initCheckDpendDisruptor(){
		Executor executor = Executors.newCachedThreadPool();
		Disruptor<TaskLogEvent> disruptor = new Disruptor<TaskLogEvent>(new TaskLogEventFactory(),ringBufferSize,executor, ProducerType.MULTI,new BlockingWaitStrategy());
		disruptor.handleEventsWith(cdh1,cdh2,cdh3,cdh4,cdh5,cdh6,cdh7,cdh8,cdh9,cdh10).then(eventFinishHandler);
		disruptor.start();
		checkDpendRB = disruptor.getRingBuffer();
	}
	
	private void initCheckRunModelDisruptor(){
		Executor executor = Executors.newCachedThreadPool();
		Disruptor<TaskLogEvent> disruptor = new Disruptor<TaskLogEvent>(new TaskLogEventFactory(),ringBufferSize,executor, ProducerType.MULTI,new BlockingWaitStrategy());
		disruptor.handleEventsWith(checkRunModelHandler).then(eventFinishHandler);
		disruptor.start();
		checkRunModelRB = disruptor.getRingBuffer();
	}
	
	private void initSend2MQDisruptor(){
		Executor executor = Executors.newCachedThreadPool();
		Disruptor<TaskLogEvent> disruptor = new Disruptor<TaskLogEvent>(new TaskLogEventFactory(),ringBufferSize,executor, ProducerType.MULTI,new BlockingWaitStrategy());
		disruptor.handleEventsWith(send2MQHandler).then(eventFinishHandler);
		disruptor.start();
		send2MQRB = disruptor.getRingBuffer();
	}
	
	private void initSucTriggerDisruptor(){
		Executor executor = Executors.newCachedThreadPool();
		Disruptor<TaskLogEvent> disruptor = new Disruptor<TaskLogEvent>(new TaskLogEventFactory(),ringBufferSize,executor, ProducerType.MULTI,new BlockingWaitStrategy());
		if(isPersonality){
			//disruptor.handleEventsWith(sucTriggerHandler,personalityEvent).then(eventFinishHandler);
			disruptor.handleEventsWith(sucTriggerHandler).then(eventFinishHandler).then(personalityEvent);
		}else{
			disruptor.handleEventsWith(sucTriggerHandler).then(eventFinishHandler);
		}		
		disruptor.start();
		sucTriggerRB = disruptor.getRingBuffer();
	}
	
	private void initFailHandlerDisruptor(){
		Executor executor = Executors.newCachedThreadPool();
		Disruptor<TaskLogEvent> disruptor = new Disruptor<TaskLogEvent>(new TaskLogEventFactory(),ringBufferSize,executor, ProducerType.MULTI,new BlockingWaitStrategy());
		disruptor.handleEventsWith(failHandler).then(eventFinishHandler);
		disruptor.start();
		failRB = disruptor.getRingBuffer();
	}
	
	private void initMQStuckHandlerDisruptor(){
		Executor executor = Executors.newCachedThreadPool();
		Disruptor<TaskLogEvent> disruptor = new Disruptor<TaskLogEvent>(new TaskLogEventFactory(),ringBufferSize,executor, ProducerType.MULTI,new BlockingWaitStrategy());
		disruptor.handleEventsWith(mQStuckHandler).then(eventFinishHandler);
		disruptor.start();
		mQStuckRB = disruptor.getRingBuffer();
	}
	
	private void initRunTimeOutHandlerDisruptor(){
		Executor executor = Executors.newCachedThreadPool();
		Disruptor<TaskLogEvent> disruptor = new Disruptor<TaskLogEvent>(new TaskLogEventFactory(),ringBufferSize,executor, ProducerType.MULTI,new BlockingWaitStrategy());
		disruptor.handleEventsWith(runTimeOutHandler).then(eventFinishHandler);
		disruptor.start();
		runTimeOutRB = disruptor.getRingBuffer();
	}
	
	public void putData(TaskLog tl,RingBuffer<TaskLogEvent> rb,int type) {
		long sequence = rb.next();
		try {
			TaskLogEvent event = rb.get(sequence);
			event.setTl(tl);
			event.setType(type);
		} finally {
			rb.publish(sequence);
		}
	}
}
