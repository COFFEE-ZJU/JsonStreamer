package cn.edu.zju.jsonStreamer.operators.storm;

import java.util.Map;

import com.google.gson.Gson;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;
import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.Constants.StormFields;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;


public abstract class OperatorOneInOneOut extends Operator{
	public OperatorOneInOneOut(JsonQueryTree tree) {
		super(tree);
	}
	protected Tuple curTuple;
	protected BasicOutputCollector curCollector;
	
	protected abstract void processMinus(MarkedElement markedElement);
	protected abstract void processPlus(MarkedElement markedElement);
	
	protected Gson gson;
	
	@Override
    public void prepare(Map stormConf, TopologyContext context) {
		gson = new Gson();
	}
	
	@Override
    public void execute(Tuple input, BasicOutputCollector collector) {
		curTuple = input;
		curCollector = collector;
		MarkedElement me = gson.fromJson(input.getStringByField(StormFields.markedElement), MarkedElement.class);
		if(me.mark == ElementMark.MINUS) processMinus(me);
			else processPlus(me);
	}
	
}
