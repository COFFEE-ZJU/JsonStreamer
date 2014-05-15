package cn.edu.zju.jsonStreamer.operators.storm;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonProjection;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.query.ProjectionDealer;
import cn.edu.zju.jsonStreamer.utils.ElementIdGenerator;


public class ProjectionOperator extends OperatorOneInOneOut{
	private ProjectionDealer projDealer;
	private final JsonProjection proj;
//	private final Map<Long, Element> synopsis;
	private Map<Long, Long> relatedMap;
	public ProjectionOperator(JsonQueryTree tree) {
		super(tree);
		proj = tree.projection;
	}
	
	@Override
    public void prepare(Map stormConf, TopologyContext context) {
		projDealer = ProjectionDealer.genProjectionDealer(proj);
//		synopsis = new HashMap<Long, Element>();
		relatedMap = new HashMap<Long, Long>();
	}
	
	@Override
	protected void processPlus(MarkedElement markedElement){
		Long id = markedElement.id;
		Element ele = markedElement.element;
		Element newEle = projDealer.deal(ele);
		long newId = ElementIdGenerator.getNewId();
//		synopsis.put(newId, newEle);
		curCollector.emit(new Values(newId, 
				new MarkedElement(newEle, newId, ElementMark.PLUS, markedElement.timeStamp)));
//		outputQueue.add(new MarkedElement(newEle, newId, ElementMark.PLUS, markedElement.timeStamp));
		relatedMap.put(id, newId);
	}
	
	@Override
	protected void processMinus(MarkedElement markedElement){
		Long id = markedElement.id;
		long eleIdToDelete = relatedMap.get(id);
//		Element eleToDelete = synopsis.get(eleIdToDelete);
		curCollector.emit(new Values(eleIdToDelete, 
				new MarkedElement(null, eleIdToDelete, ElementMark.MINUS, markedElement.timeStamp)));
//		outputQueue.add(new MarkedElement(eleToDelete, eleIdToDelete, ElementMark.MINUS, markedElement.timeStamp));
		relatedMap.remove(id);
//		synopsis.remove(eleIdToDelete);
	}
}
