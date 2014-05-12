package cn.edu.zju.jsonStreamer.operators.storm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.Constants.StormFields;
import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.query.ProjectionDealer;
import cn.edu.zju.jsonStreamer.utils.ElementIdGenerator;


public class JoinOperator extends Operator{
	private static enum Source {LEFT, RIGHT};
	private BasicOutputCollector curCollector;
	
	private final Map<Integer, Set<Long> > leftGroupKeyMap, rightGroupKeyMap;
	private final boolean leftOutter, rightOutter;
	private final Map<Long, Set<Long> > leftRelatedMapper, rightRelatedMapper;	
	//map to those elements which are related to the input element
	private final Map<Long, Element> leftElements, rightElements;
	private final Set<Long> synopsis;
//	private final JoinEqDealer eqDealer;
	private final ProjectionDealer projDealer;
	
//	private Queue<MarkedElement> leftInputQueue = null, rightInputQueue = null;
//	private Queue<MarkedElement> outputQueue = null;
	public JoinOperator(JsonQueryTree tree) {
		super(tree);
		leftGroupKeyMap = new HashMap<Integer, Set<Long>>();
		rightGroupKeyMap = new HashMap<Integer, Set<Long>>();
		
		leftRelatedMapper = new HashMap<Long, Set<Long> >();
		rightRelatedMapper = new HashMap<Long, Set<Long> >();
		synopsis = new HashSet<Long>();
		leftElements = new HashMap<Long, Element>();
		rightElements = new HashMap<Long, Element>();
		
		leftOutter = rightOutter = false; 	//TODO
		
//		eqDealer = new JoinEqDealer(tree.left_join_attribute, tree.right_join_attribute);
		projDealer = ProjectionDealer.genProjectionDealer(tree.projection);
	}
	
	private void processPlus(Element keyEle, MarkedElement markedElement, Source source){
		Long id = markedElement.id;
		Element ele = markedElement.element;
		Map<Long, Set<Long> > mapper;
		Set<Long> otherSideElements;
		Map<Long, Element> eleMap;
		int key = keyEle.hashCode();
		Map<Integer, Set<Long> > groupKeyMap;
		
		if(source == Source.LEFT){
			groupKeyMap = leftGroupKeyMap;
			
			leftElements.put(id, ele);
			mapper = leftRelatedMapper;
			otherSideElements = rightGroupKeyMap.get(key);
			eleMap = rightElements;
		}
		else{
			groupKeyMap = rightGroupKeyMap;
			
			rightElements.put(id, ele);
			mapper = rightRelatedMapper;
			otherSideElements = leftGroupKeyMap.get(key);
			eleMap = leftElements;
		}
		if(! groupKeyMap.containsKey(key)){
			Set<Long> newSet = new HashSet<Long>();
			newSet.add(id);
			groupKeyMap.put(key, newSet);
		}
		else groupKeyMap.get(key).add(id);
		
		if(! otherSideElements.isEmpty()){
			Iterator<Long> it = otherSideElements.iterator();
			Element currentEle;
			Element newEle;
			long newId;
			
			Set<Long> set = new HashSet<Long>();
			while(it.hasNext()){
				currentEle = eleMap.get(it.next());
				if(source == Source.LEFT) newEle = projDealer.deal(ele, currentEle);
				else newEle = projDealer.deal(currentEle, ele);
				newId = ElementIdGenerator.getNewId();
				set.add(newId);
				synopsis.add(newId);
				curCollector.emit(new Values(
						new MarkedElement(newEle, newId, ElementMark.PLUS, markedElement.timeStamp)));
			}
			mapper.put(id, set);
		}
		
//		Set<Long> set = new HashSet<Long>();
//		it = otherSideElements.iterator();
//		
//		boolean test;
//		while(it.hasNext()){
//			currentEle = it.next();
//			if(source == Source.LEFT) test = eqDealer.deal(ele, currentEle);
//			else test = eqDealer.deal(currentEle, ele);
//			
//			if(test){
//				Element newEle;
//				long newId;
//				if(source == Source.LEFT) newEle = projDealer.deal(ele, currentEle);
//				else newEle = projDealer.deal(currentEle, ele);
//				
//				newId = ElementIdGenerator.getNewId();
//				synopsis.put(newId, newEle);
//				set.add(newId);
//				
//				outputQueue.add(new MarkedElement(newEle, newId, ElementMark.PLUS, markedElement.timeStamp));
//			}
//			
//		}
//		
//		if(! set.isEmpty()) mapper.put(id, set);
	}
	
	private void processMinus(Element keyEle, MarkedElement markedElement, Source source){
		Long id = markedElement.id;
		Map<Long, Set<Long> > mapper;
		Map<Integer, Set<Long> > groupKeyMap;
		int key = keyEle.hashCode();
		
		if(source == Source.LEFT){
			groupKeyMap = leftGroupKeyMap;
			leftElements.remove(id);
			mapper = leftRelatedMapper;
		}
		else{
			groupKeyMap = rightGroupKeyMap;
			rightElements.remove(id);
			mapper = rightRelatedMapper;
		}
		Set<Long> set = groupKeyMap.get(key);
		set.remove(id);
		if(set.isEmpty()) groupKeyMap.remove(key);
		
		if(! mapper.containsKey(id)) return ;		//no elements related to this one
		
		Iterator<Long> it = mapper.get(id).iterator();
		long eleIdToDelete;
//		Element eleToDelete;
		while(it.hasNext()){
			eleIdToDelete = it.next();
			if(! synopsis.contains(eleIdToDelete)) continue;		//already deleted
//			eleToDelete = synopsis.get(eleIdToDelete);
			synopsis.remove(eleIdToDelete);
			
			curCollector.emit(new Values(
					new MarkedElement(null, eleIdToDelete, ElementMark.MINUS, markedElement.timeStamp)));
//			outputQueue.add(new MarkedElement(eleToDelete, eleIdToDelete, ElementMark.MINUS, markedElement.timeStamp));
		}
		
		mapper.remove(id);
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		curCollector = collector;
		Source source;
		int sourceId = input.getIntegerByField(StormFields.sourceId);
		if(sourceId == 1) source = Source.LEFT;
		else if(sourceId == 2) source = Source.RIGHT;
		else throw new RuntimeException("source id worng!!!");
		
		Element keyEle = (Element)input.getValueByField(StormFields.groupKey);
		
		MarkedElement me = (MarkedElement)input.getValueByField(StormFields.markedElement);
		if(me.mark == ElementMark.MINUS) processMinus(keyEle, me, source);
		else processPlus(keyEle, me, source);
		
//		while(! leftInputQueue.isEmpty() && ! rightInputQueue.isEmpty()){
//			MarkedElement leftMe = leftInputQueue.peek();
//			MarkedElement rightMe = rightInputQueue.peek();
//			if(leftMe.timeStamp <= rightMe.timeStamp){
//				leftInputQueue.poll();
//				if(leftMe.mark == ElementMark.MINUS) processMinus(leftMe, Source.LEFT);
//				else processPlus(leftMe, Source.LEFT);
//			}
//			else{
//				rightInputQueue.poll();
//				if(rightMe.mark == ElementMark.MINUS) processMinus(rightMe, Source.RIGHT);
//				else processPlus(rightMe, Source.RIGHT);
//			}
//		}
//		
//		MarkedElement me;
//		while(! leftInputQueue.isEmpty()){
//			me = leftInputQueue.poll();
//			if(me.mark == ElementMark.MINUS) processMinus(me, Source.LEFT);
//			else processPlus(me, Source.LEFT);
//		}
//		while(! rightInputQueue.isEmpty()){
//			me = rightInputQueue.poll();
//			if(me.mark == ElementMark.MINUS) processMinus(me, Source.RIGHT);
//			else processPlus(me, Source.RIGHT);
//		}
		
	}

}
