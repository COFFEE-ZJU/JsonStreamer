package cn.edu.zju.jsonStreamer.operators.storm;

import java.util.Map;
import com.google.gson.Gson;

import redis.clients.jedis.Jedis;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import cn.edu.zju.jsonStreamer.constants.Constants.StormFields;
import cn.edu.zju.jsonStreamer.json.MarkedElement;


public class RootOperator extends BaseBasicBolt{
	String redisHost;
	String queueName;
	Jedis client;
	Gson gson;
	
	public RootOperator(String host, String queue) {
		redisHost = host;
		queueName = queue;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
    public void prepare(Map stormConf, TopologyContext context) {
		client = new Jedis(redisHost);
		gson = new Gson();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
//		MarkedElement me = (MarkedElement)input.getValueByField(StormFields.markedElement);
		MarkedElement me = gson.fromJson(input.getStringByField(StormFields.markedElement), MarkedElement.class);
		client.rpush(queueName, gson.toJson(me.element.jsonElement));
	}
	
	@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
