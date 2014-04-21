package operators;

import constants.Constants.ElementMark;
import json.MarkedElement;
import jsonAPI.JsonQueryTree;

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
