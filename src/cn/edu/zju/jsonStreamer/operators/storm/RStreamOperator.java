package cn.edu.zju.jsonStreamer.operators.storm;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.Constants.StormFields;
import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;


public class RStreamOperator extends Operator{
	private final List<Element> eleSynopsis;
	private final List<Long> idSynopsis;
	protected Gson gson;
	
	public RStreamOperator(JsonQueryTree tree) {
		super(tree);
		eleSynopsis = new LinkedList<Element>();
		idSynopsis = new LinkedList<Long>();
	}
	
	@Override
    public void prepare(Map stormConf, TopologyContext context) {
		gson = new Gson();
	}

	private void output(MarkedElement markedElement, BasicOutputCollector collector) {
		long now = markedElement.timeStamp;
		Iterator<Long> it1 = idSynopsis.iterator();
		Iterator<Element> it2 = eleSynopsis.iterator();
		long id;
		Element ele;
		while(it1.hasNext()){
			id = it1.next();
			ele = it2.next();
			collector.emit(new Values(id, 
					gson.toJson(new MarkedElement(ele, id, ElementMark.UNMARKED, now))));
		}
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
//		MarkedElement me = (MarkedElement)input.getValueByField(StormFields.markedElement);
		MarkedElement me = gson.fromJson(input.getStringByField(StormFields.markedElement), MarkedElement.class);
		if(me.mark == ElementMark.PLUS){
			eleSynopsis.add(me.element);
			idSynopsis.add(me.id);
			output(me, collector);
		}
		else {
			if(! idSynopsis.remove(me.id)) throw new RuntimeException("id not found!!!");
			if(! eleSynopsis.remove(me.element)) throw new RuntimeException("element not found!!!");
			output(me, collector);
		}
	}

}
