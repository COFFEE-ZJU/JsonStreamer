package cn.edu.zju.jsonStreamer.operators.storm;

import redis.clients.jedis.Jedis;
import cn.edu.zju.jsonStreamer.IO.IOManager;
import cn.edu.zju.jsonStreamer.IO.input.JStreamInput;
import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.utils.ElementIdGenerator;
import cn.edu.zju.jsonStreamer.utils.TimeStampGenerator;

public class StreamSender extends cn.edu.zju.jsonStreamer.operators.Operator{
	private boolean isMaster = false;	//TODO
	private JStreamInput inputStream;
	private final String queueName;
	private final Jedis jedis;
	
	public StreamSender(JsonQueryTree tree, long queueId) throws SystemErrorException {
		super(tree);
		inputStream = IOManager.getInstance().getInputStreamByName(tree.stream_source);
		inputStream.execute();
		isMaster = tree.is_master;
		queueName = Constants.stormJedisSendQueue+queueId;
		jedis = new Jedis("127.0.0.1");
	}
	
	public String getQueueName(){
		return queueName;
	}

	@Override
	public void execute() throws SystemErrorException {
		while(! inputStream.isEmpty()){
			jedis.rpush(queueName, 
					""+ElementIdGenerator.getNewId()+"|"+
					TimeStampGenerator.getCurrentTimeStamp()+"|"+
					inputStream.getNextElement());
		}
		
	}

}