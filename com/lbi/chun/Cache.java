package com.autonavi.chun;


import java.util.concurrent.ConcurrentLinkedQueue;

import com.alibaba.odps.tunnel.io.Record;


public class Cache {

	public static ConcurrentLinkedQueue<Record> stack=new ConcurrentLinkedQueue<Record>();
}
