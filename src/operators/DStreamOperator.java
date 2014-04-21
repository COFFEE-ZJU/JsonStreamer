package operators;

import constants.Constants.ElementMark;
import json.MarkedElement;
import jsonAPI.JsonQueryTree;

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
