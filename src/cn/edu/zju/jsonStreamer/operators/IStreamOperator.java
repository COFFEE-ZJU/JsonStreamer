package cn.edu.zju.jsonStreamer.operators;

import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;

public class IStreamOperator extends OperatorOneInOneOut{
	
	public IStreamOperator(JsonQueryTree tree) {
		super(tree);
	}

	@Override
	protected void processMinus(MarkedElement markedElement) {
	}

	@Override
	protected void processPlus(MarkedElement markedElement) {
		markedElement.mark = ElementMark.UNMARKED;
		outputQueue.add(markedElement);
	}
}
