package cn.edu.zju.jsonStreamer.operators;

import java.util.Queue;

import cn.edu.zju.jsonStreamer.IO.IOManager;
import cn.edu.zju.jsonStreamer.IO.JStreamInput;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.utils.ElementIdGenerator;
import cn.edu.zju.jsonStreamer.utils.JsonSchemaUtils;
import cn.edu.zju.jsonStreamer.utils.TimeStampGenerator;


public class LeafOperator extends Operator{
	private boolean isMaster = false;
	private JStreamInput inputStream;
	private Queue<MarkedElement> outputQueue = null;
	
	public LeafOperator(JsonQueryTree tree) throws SystemErrorException{
		super(tree);
		inputStream = IOManager.getInstance().getInputStreamByName(tree.stream_source);
		inputStream.execute();
		isMaster = tree.is_master;
	}
	
	@Override
	public void execute() throws SystemErrorException {
		if(outputQueue == null){
			if(outputQueueList.size() != 1){
				System.err.println(outputQueueList.size());
				throw new SystemErrorException("queue size abnormal");
			}
			outputQueue = outputQueueList.get(0);
		}
		Element ele;
		while(! inputStream.isEmpty()){
			ele = inputStream.getNextElement();
			if(ele == null) return ;
			if(! JsonSchemaUtils.validate(ele.jsonElement, schema)) continue ;
			outputQueue.add(new MarkedElement(ele, 
					ElementIdGenerator.getNewId(), 
					ElementMark.PLUS, 
					TimeStampGenerator.getCurrentTimeStamp()));
		}
	}

}
