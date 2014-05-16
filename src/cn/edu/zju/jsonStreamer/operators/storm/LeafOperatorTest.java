package cn.edu.zju.jsonStreamer.operators.storm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.Constants.JsonValueType;
import cn.edu.zju.jsonStreamer.constants.Constants.StormFields;
import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.json.JsonSchema;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.utils.JsonSchemaUtils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class LeafOperatorTest extends BaseRichSpout {
	String redisHost;
	String queueName;
	Queue<String> messages;
//	BlockingQueue<String> messages;
	SpoutOutputCollector collector;
	Gson gson;
	JsonSchema schema;
	public LeafOperatorTest(JsonQueryTree tree, String host, String queue){
		redisHost = host;
		queueName = queue;
		schema = tree.schema;
		messages = new LinkedList<String>();
	}
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		gson = new Gson();
//		messages = new ArrayBlockingQueue<String>(Short.MAX_VALUE);
		new Thread(new Runnable() {
			@Override
			public void run() {
				while(true){
					try{
						Jedis client = new Jedis(redisHost);
						List<String> res = client.blpop(Integer.MAX_VALUE, queueName);
						if(res != null)
							messages.offer(res.get(1));
					}catch(Exception e){
						e.printStackTrace();
						try {
							Thread.sleep(100);
						} catch (InterruptedException e1) {}
					}
				}
			}}).start();
	}

	@Override
	public void nextTuple() {
		while(!messages.isEmpty()){
			String msg = messages.poll();
			if(Constants.DEBUG) System.out.println("leafOp: "+msg);
			File file = new File("/home/coffee/storm/debug.txt");
			try {
				BufferedWriter bw = new BufferedWriter(new FileWriter(file));
				bw.append("test msg");
				bw.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			try{
//				int pos1 = msg.indexOf('|');
//				int pos2 = msg.indexOf('|', pos1+1);
//				if(pos1 == -1 || pos2 == -1) continue;
//				long id = Long.valueOf(msg.substring(0, pos1));
//				long timeStamp = Long.valueOf(msg.substring(pos1+1, pos2));
//				String json = msg.substring(pos2+1);
//				JsonElement ele = gson.fromJson(json, JsonElement.class);
//				
//				if(! JsonSchemaUtils.validate(ele, schema)) continue;
//				MarkedElement me = new MarkedElement(new Element(ele, schema.getType()), id, ElementMark.PLUS, timeStamp);
//				collector.emit(new Values(id, me));
			} catch (Exception e){
				file = new File("/home/coffee/storm/debug.txt");
				try {
					BufferedWriter bw = new BufferedWriter(new FileWriter(file));
					bw.append(e.getMessage());
					bw.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				continue;
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(StormFields.id, StormFields.markedElement));
	}

}