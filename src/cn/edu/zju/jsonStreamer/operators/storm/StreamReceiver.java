package cn.edu.zju.jsonStreamer.operators.storm;

import java.util.Queue;

import redis.clients.jedis.Jedis;
import cn.edu.zju.jsonStreamer.IO.IOManager;
import cn.edu.zju.jsonStreamer.IO.input.JStreamInput;
import cn.edu.zju.jsonStreamer.IO.output.JStreamOutput;
import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.utils.ElementIdGenerator;
import cn.edu.zju.jsonStreamer.utils.TimeStampGenerator;

public class StreamReceiver extends cn.edu.zju.jsonStreamer.operators.Operator{
	protected final JStreamOutput outputStream;
//	protected Queue<MarkedElement> inputQueue = null;
	private final String queueName;
	private final Jedis jedis;
	
	public StreamReceiver(JsonQueryTree tree, JStreamOutput outputStream, long queueId) {
		super(tree);
		this.outputStream = outputStream;
		queueName = Constants.stormJedisReceiveQueue+queueId;
		jedis = new Jedis("127.0.0.1");
	}
	
	public String getQueueName(){
		return queueName;
	}
	
	@Override
	public final void execute() throws SystemErrorException{
		String json;
		while(true){
			json = jedis.lpop(queueName);
			if(json == null) break;
			outputStream.pushNext(json);
		}
	}

}
