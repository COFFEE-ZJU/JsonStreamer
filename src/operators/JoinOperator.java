package operators;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import constants.SystemErrorException;
import constants.Constants.ElementMark;

import query.JoinEqDealer;
import query.ProjectionDealer;
import utils.ElementIdGenerator;

import json.Element;
import json.MarkedElement;
import jsonAPI.JsonQueryTree;

public class JoinOperator extends Operator{
	private static enum Source {LEFT, RIGHT};
	
	private final boolean leftOutter, rightOutter;
	private final Map<Long, Set<Long> > leftRelatedMapper, rightRelatedMapper;	
	//map to those elements which are related to the input element
	private final Map<Long, Element> leftElements, rightElements;
	private final Map<Long, Element> synopsis;
	private final JoinEqDealer eqDealer;
	private final ProjectionDealer projDealer;
	
	private Queue<MarkedElement> leftInputQueue = null, rightInputQueue = null;
	private Queue<MarkedElement> outputQueue = null;
	public JoinOperator(JsonQueryTree tree) {
		super(tree);
		leftRelatedMapper = new HashMap<Long, Set<Long> >();
		rightRelatedMapper = new HashMap<Long, Set<Long> >();
		synopsis = new HashMap<Long, Element>();
		leftElements = new HashMap<Long, Element>();
		rightElements = new HashMap<Long, Element>();
		
		leftOutter = rightOutter = false; 	//TODO
		
		eqDealer = new JoinEqDealer(tree.left_join_attribute, tree.right_join_attribute);
		projDealer = ProjectionDealer.genProjectionDealer(tree.projection);
	}
	
	private void processPlus(MarkedElement markedElement, Source source){
		Long id = markedElement.id;
		Element ele = markedElement.element;
		Map<Long, Set<Long> > mapper;
		Collection<Element> otherSideElements;
		Iterator<Element> it;
		if(source == Source.LEFT){
			leftElements.put(id, ele);
			mapper = leftRelatedMapper;
			otherSideElements = rightElements.values();
		}
		else{
			rightElements.put(id, ele);
			mapper = rightRelatedMapper;
			otherSideElements = leftElements.values();
		}
		
		Set<Long> set = new HashSet<Long>();
		it = otherSideElements.iterator();
		Element currentEle;
		boolean test;
		while(it.hasNext()){
			currentEle = it.next();
			if(source == Source.LEFT) test = eqDealer.deal(ele, currentEle);
			else test = eqDealer.deal(currentEle, ele);
			
			if(test){
				Element newEle;
				long newId;
				if(source == Source.LEFT) newEle = projDealer.deal(ele, currentEle);
				else newEle = projDealer.deal(currentEle, ele);
				
				newId = ElementIdGenerator.getNewId();
				synopsis.put(newId, newEle);
				set.add(newId);
				
				outputQueue.add(new MarkedElement(newEle, newId, ElementMark.PLUS, markedElement.timeStamp));
			}
			
		}
		
		if(! set.isEmpty()) mapper.put(id, set);
	}
	
	private void processMinus(MarkedElement markedElement, Source source){
		Long id = markedElement.id;
		Map<Long, Set<Long> > mapper;
		if(source == Source.LEFT){
			leftElements.remove(id);
			mapper = leftRelatedMapper;
		}
		else{
			rightElements.remove(id);
			mapper = rightRelatedMapper;
		}
		
		if(! mapper.containsKey(id)) return ;		//no elements related to this one
		
		Iterator<Long> it = mapper.get(id).iterator();
		long eleIdToDelete;
		Element eleToDelete;
		while(it.hasNext()){
			eleIdToDelete = it.next();
			if(! synopsis.containsKey(eleIdToDelete)) continue;		//already deleted
			eleToDelete = synopsis.get(eleIdToDelete);
			synopsis.remove(eleToDelete);
			outputQueue.add(new MarkedElement(eleToDelete, eleIdToDelete, ElementMark.MINUS, markedElement.timeStamp));
		}
		
		mapper.remove(id);
	}

	@Override
	public void execute() {
		if(leftInputQueue == null || rightInputQueue == null || outputQueue == null){
			if(inputQueueList.size() != 2 || outputQueueList.size() != 1)
				throw new SystemErrorException("queue size abnormal");
			leftInputQueue = inputQueueList.get(0);
			rightInputQueue = inputQueueList.get(1);
			outputQueue = outputQueueList.get(0);
		}
		while(! leftInputQueue.isEmpty() && ! rightInputQueue.isEmpty()){
			MarkedElement leftMe = leftInputQueue.peek();
			MarkedElement rightMe = rightInputQueue.peek();
			if(leftMe.timeStamp <= rightMe.timeStamp){
				leftInputQueue.poll();
				if(leftMe.mark == ElementMark.MINUS) processMinus(leftMe, Source.LEFT);
				else processPlus(leftMe, Source.LEFT);
			}
			else{
				rightInputQueue.poll();
				if(rightMe.mark == ElementMark.MINUS) processMinus(rightMe, Source.RIGHT);
				else processPlus(rightMe, Source.RIGHT);
			}
		}
		
		MarkedElement me;
		while(! leftInputQueue.isEmpty()){
			me = leftInputQueue.poll();
			if(me.mark == ElementMark.MINUS) processMinus(me, Source.LEFT);
			else processPlus(me, Source.LEFT);
		}
		while(! rightInputQueue.isEmpty()){
			me = rightInputQueue.poll();
			if(me.mark == ElementMark.MINUS) processMinus(me, Source.RIGHT);
			else processPlus(me, Source.RIGHT);
		}
		
	}

}
