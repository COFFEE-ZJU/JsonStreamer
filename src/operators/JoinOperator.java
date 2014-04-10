package operators;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import query.JoinEqDealer;

import json.Element;
import jsonAPI.JsonQueryTree;

public class JoinOperator extends Operator{
	private static enum Source {LEFT, RIGHT};
	
	private final boolean leftOutter, rightOutter;
	private final Map<Long, Set<Long> > leftMapper, rightMapper;
	private final Map<Long, Element> leftElements, rightElements;
	private final Map<Long, Element> synopsis;
	private final JoinEqDealer eqDealer;
	public JoinOperator(JsonQueryTree tree) {
		super(tree);
		leftMapper = new HashMap<Long, Set<Long> >();
		rightMapper = new HashMap<Long, Set<Long> >();
		synopsis = new HashMap<Long, Element>();
		leftElements = new HashMap<Long, Element>();
		rightElements = new HashMap<Long, Element>();
		
		leftOutter = rightOutter = false; 	//TODO
		
		eqDealer = new JoinEqDealer(tree.left_join_attribute, tree.right_join_attribute);
	}
	
	private void processPlus(long id, Element ele, Source source){
		Map<Long, Set<Long> > mapper;
		Collection<Element> otherSideElements;
		Iterator<Element> it;
		if(source == Source.LEFT){
			leftElements.put(id, ele);
			mapper = leftMapper;
			otherSideElements = rightElements.values();
		}
		else{
			rightElements.put(id, ele);
			mapper = rightMapper;
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
			
			if(test) ;//project and add this
			
		}
		
		if(! set.isEmpty()) mapper.put(id, set);
	}
	
	private void processMinus(long id, Source source){
		Map<Long, Set<Long> > mapper;
		if(source == Source.LEFT){
			leftElements.remove(id);
			mapper = leftMapper;
		}
		else{
			rightElements.remove(id);
			mapper = rightMapper;
		}
		
		if(! mapper.containsKey(id)) return ;		//no elements related to this one
		
		Iterator<Long> it = mapper.get(id).iterator();
		long currentId;
		Element currentEle;
		while(it.hasNext()){
			currentId = it.next();
			if(! synopsis.containsKey(currentId)) continue;		//already deleted
			currentEle = synopsis.get(currentId);
			synopsis.remove(currentId);
			//TODO output the minus marked element
		}
		
		mapper.remove(id);
	}

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}

}
