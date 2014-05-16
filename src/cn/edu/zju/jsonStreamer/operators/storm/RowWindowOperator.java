package cn.edu.zju.jsonStreamer.operators.storm;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.google.gson.Gson;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.Constants.StormFields;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;

public class RowWindowOperator extends Operator{
	private final int rowSize;		//0 for now; -1 for unbounded
	private Queue<Long> synopsis;
	
	protected Gson gson;
	
	public RowWindowOperator(JsonQueryTree tree){
		super(tree);
		rowSize = ((Double)tree.windowsize).intValue();
	}
	

	@Override
    public void prepare(Map stormConf, TopologyContext context) {
		gson = new Gson();
		synopsis = new LinkedList<Long>();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
//		MarkedElement me = (MarkedElement)input.getValueByField(StormFields.markedElement);
		MarkedElement me = gson.fromJson(input.getStringByField(StormFields.markedElement), MarkedElement.class);
		if(me.mark == ElementMark.MINUS) throw new RuntimeException("stream element can't be MINUS marked");
		
		if(synopsis.size() == rowSize){
			long eleIdToDelete = synopsis.poll();
			collector.emit(new Values(eleIdToDelete, 
					gson.toJson(new MarkedElement(null, eleIdToDelete, ElementMark.MINUS, me.timeStamp))));
		}
		synopsis.add(me.id);
		collector.emit(input.getValues());
	}

}
