package cn.edu.zju.jsonStreamer.operators;

import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;

public class DStreamOperator extends OperatorOneInOneOut{
	public DStreamOperator(JsonQueryTree tree) {
		super(tree);
	}

	@Override
	protected void processMinus(MarkedElement markedElement) {
		markedElement.mark = ElementMark.UNMARKED;
		outputQueue.add(markedElement);
	}

	@Override
	protected void processPlus(MarkedElement markedElement) {
	}
}
