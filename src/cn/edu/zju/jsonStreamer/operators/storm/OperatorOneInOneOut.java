package cn.edu.zju.jsonStreamer.operators.storm;

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
	
	@Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
		curTuple = tuple;
		curCollector = collector;
		MarkedElement me = (MarkedElement)tuple.getValueByField(StormFields.markedElement);
		if(me.mark == ElementMark.MINUS) processMinus(me);
			else processPlus(me);
	}
	
}
