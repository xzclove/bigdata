package com.xzc.demo.test.bean;

/**
 * @Desc 默认对象
 * @Author feelingxu@tcl.com:
 * @Date 创建时间：2016年6月8日 下午3:06:11
 * @Version V1.0.0
 */
public class Demo {

	private String id;
	private String name;

	public Demo() {
	}

	public Demo(String id, String name) {
		this.id = id;
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
