package com.asiainfo.dacp.dp.server.scheduler.bean;

import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * 表信息
 * @author zhangqi
 *
 */
@Getter
@Setter
public class TableInfo  implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7976481797147822009L;
	private String id;
	private String name;
	private String cnName;
	private String cycleType;
}
