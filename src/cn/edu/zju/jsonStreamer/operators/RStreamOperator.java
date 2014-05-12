package cn.edu.zju.jsonStreamer.operators;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;


public class RStreamOperator extends OperatorOneInOneOut{
	private final List<Element> synopsis;
	public RStreamOperator(JsonQueryTree tree) {
		super(tree);
		synopsis = new LinkedList<Element>();
	}

	private void output(MarkedElement markedElement) {
		long now = markedElement.timeStamp;
		Iterator<Element> it = synopsis.iterator();
		Element ele;
		while(it.hasNext()){
			ele = it.next();
			outputQueue.add(new MarkedElement(ele, -1, ElementMark.UNMARKED, now));
		}
	}

	@Override
	protected void processMinus(MarkedElement markedElement) throws SystemErrorException {
		if(! synopsis.remove(markedElement.element))
			throw new SystemErrorException("element not found!!!");
		
		output(markedElement);
	}

	@Override
	protected void processPlus(MarkedElement markedElement) {
		synopsis.add(markedElement.element);
		output(markedElement);
	}

}
