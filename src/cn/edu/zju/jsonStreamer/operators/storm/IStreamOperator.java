package cn.edu.zju.jsonStreamer.operators.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.Constants.StormFields;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;

public class IStreamOperator extends Operator{
	public IStreamOperator(JsonQueryTree tree) {
		super(tree);
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		MarkedElement me = (MarkedElement)input.getValueByField(StormFields.markedElement);
		if(me.mark == ElementMark.PLUS){
			me.mark = ElementMark.UNMARKED;
			collector.emit(new Values(me.id, me));
		}
	}
}
