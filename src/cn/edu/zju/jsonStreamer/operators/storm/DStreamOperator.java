package cn.edu.zju.jsonStreamer.operators.storm;

import java.util.Map;

import com.google.gson.Gson;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.Constants.StormFields;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;

public class DStreamOperator extends Operator{
	protected Gson gson;
	
	public DStreamOperator(JsonQueryTree tree) {
		super(tree);
	}
	
	@Override
    public void prepare(Map stormConf, TopologyContext context) {
		gson = new Gson();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
//		MarkedElement me = (MarkedElement)input.getValueByField(StormFields.markedElement);
		String meString = input.getStringByField(StormFields.markedElement);
		MarkedElement me = gson.fromJson(meString, MarkedElement.class);
		if(me.mark == ElementMark.MINUS){
			me.mark = ElementMark.UNMARKED;
			collector.emit(new Values(me.id, meString));
		}
	}
}
