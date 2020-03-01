package com.asiainfo.dacp.dp.server.scheduler.type;

/**
 * 依赖配置类型
 * @author zhangqi
 *
 */
public enum DataFreq {
	/***无*/
	N,
	/***年*/
	Y,
	/***天*/
	D,
	/***周*/
	W,
	/***月*/
	M,
	/***月末*/
	ML,
	/***小时*/
	H,
	/***23点*/
	DL,
	/***分钟*/
	MI,
	/***指定小时*/
	DH,
	/***指定日*/
	DD;
}
