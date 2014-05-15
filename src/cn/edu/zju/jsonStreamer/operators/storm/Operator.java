package cn.edu.zju.jsonStreamer.operators.storm;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import cn.edu.zju.jsonStreamer.constants.Constants.StormFields;
import cn.edu.zju.jsonStreamer.json.JsonSchema;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;

public abstract class Operator extends BaseBasicBolt{
	protected JsonSchema schema;
	
//	private OperatorState state;
//	private void setState(OperatorState state){
//		this.state = state;
//	}
	
	public Operator(JsonQueryTree tree){
		schema = tree.schema;
	}
	
	@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(StormFields.id, StormFields.markedElement));
    }
	
//	public OperatorState getState(){
//		return state;
//	}
}
