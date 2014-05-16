package cn.edu.zju.jsonStreamer.operators;

import java.util.HashMap;
import java.util.Map;

import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.query.ProjectionDealer;
import cn.edu.zju.jsonStreamer.utils.ElementIdGenerator;


public class ProjectionOperator extends OperatorOneInOneOut{
	private final ProjectionDealer projDealer;
	private final Map<Long, Element> synopsis;
	private final Map<Long, Long> relatedMap;
	public ProjectionOperator(JsonQueryTree tree) {
		super(tree);
		projDealer = ProjectionDealer.genProjectionDealer(tree.projection);
		synopsis = new HashMap<Long, Element>();
		relatedMap = new HashMap<Long, Long>();
	}
	
	@Override
	protected void processPlus(MarkedElement markedElement) throws SystemErrorException{
		Long id = markedElement.id;
		Element ele = markedElement.element;
		Element newEle = projDealer.deal(ele);
		long newId = ElementIdGenerator.getNewId();
		synopsis.put(newId, newEle);
		output(new MarkedElement(newEle, newId, ElementMark.PLUS, markedElement.timeStamp));
		relatedMap.put(id, newId);
	}
	
	@Override
	protected void processMinus(MarkedElement markedElement) throws SystemErrorException{
		Long id = markedElement.id;
		long eleIdToDelete = relatedMap.get(id);
		Element eleToDelete = synopsis.get(eleIdToDelete);
		output(new MarkedElement(eleToDelete, eleIdToDelete, ElementMark.MINUS, markedElement.timeStamp));
		relatedMap.remove(id);
		synopsis.remove(eleIdToDelete);
	}
}
