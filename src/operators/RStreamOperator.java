package operators;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import constants.SystemErrorException;
import constants.Constants.ElementMark;

import IO.JStreamOutput;
import json.Element;
import json.MarkedElement;
import jsonAPI.JsonQueryTree;

public class RStreamOperator extends OperatorRelationToStream{
	private final List<Element> synopsis;
	public RStreamOperator(JsonQueryTree tree, JStreamOutput outputStream) {
		super(tree, outputStream);
		synopsis = new LinkedList<Element>();
	}

	@Override
	protected void process(MarkedElement markedElement) {
		if(markedElement.mark == ElementMark.MINUS){
			if(! synopsis.remove(markedElement.element))
				throw new SystemErrorException("element not found!!!");
		}
		else
			synopsis.add(markedElement.element);
		
		long now = markedElement.timeStamp;
		Iterator<Element> it = synopsis.iterator();
		Element ele;
		while(it.hasNext()){
			ele = it.next();
			outputStream.pushNext(new MarkedElement(ele, -1, ElementMark.UNMARKED, now));
		}
	}

}
