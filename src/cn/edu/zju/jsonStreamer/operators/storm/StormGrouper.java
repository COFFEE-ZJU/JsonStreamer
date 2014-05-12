package cn.edu.zju.jsonStreamer.operators.storm;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.Constants.StormFields;
import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonExpression;
import cn.edu.zju.jsonStreamer.query.ExpressionDealer;

public class StormGrouper extends BaseBasicBolt{
	private Map<Long, Element> synopsis;
	private ExpressionDealer groupKeyDealer;
	private Integer sourceId = null;
	public StormGrouper(JsonExpression keyExpr){
		this(keyExpr, null);
	}
	public StormGrouper(JsonExpression keyExpr, Integer sourceId){
		synopsis = new HashMap<Long, Element>();
		groupKeyDealer = ExpressionDealer.genExpressionDealer(keyExpr);
		this.sourceId = sourceId;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		long id = input.getLongByField(StormFields.id);
		MarkedElement me = (MarkedElement)input.getValueByField(StormFields.markedElement);
		Element keyEle;
		if(me.mark == ElementMark.PLUS){
			keyEle = groupKeyDealer.deal(me.element);
			synopsis.put(id, keyEle);
		}
		else{
			keyEle = synopsis.get(id);
			synopsis.remove(id);
		}
		
		if(sourceId == null)
			collector.emit(new Values(id, keyEle, me));
		else collector.emit(new Values(sourceId, id, keyEle, me));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if(sourceId == null)
			declarer.declare(new Fields(StormFields.id, StormFields.groupKey, StormFields.markedElement));
		else
			declarer.declare(new Fields(StormFields.sourceId, StormFields.id, StormFields.groupKey, StormFields.markedElement));
	}

}
