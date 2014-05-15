package cn.edu.zju.jsonStreamer.operators.storm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
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

public class LeafOperator extends BaseRichSpout {
	String redisHost;
	String queueName;
	BlockingQueue<String> messages;
	SpoutOutputCollector collector;
	Gson gson;
	JsonSchema schema;
	public LeafOperator(JsonQueryTree tree, String host, String queue){
		redisHost = host;
		queueName = queue;
		schema = tree.schema;
	}
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		gson = new Gson();
		messages = new ArrayBlockingQueue<String>(Short.MAX_VALUE);
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
			try{
				int pos1 = msg.indexOf('|');
				int pos2 = msg.indexOf('|', pos1+1);
				if(pos1 == -1 || pos2 == -1) continue;
				long id = Long.valueOf(msg.substring(0, pos1));
				long timeStamp = Long.valueOf(msg.substring(pos1+1, pos2));
				String json = msg.substring(pos2+1);
				JsonElement ele = gson.fromJson(json, JsonElement.class);
				
				if(! JsonSchemaUtils.validate(ele, schema)) continue;
				MarkedElement me = new MarkedElement(new Element(ele, schema.getType()), id, ElementMark.PLUS, timeStamp);
				collector.emit(new Values());
			} catch (Exception e){continue; }
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(StormFields.id, StormFields.markedElement));
	}

}