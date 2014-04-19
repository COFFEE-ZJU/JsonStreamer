package operators;

import IO.JStreamOutput;

import constants.Constants.ElementMark;
import json.MarkedElement;
import jsonAPI.JsonQueryTree;

public class DStreamOperator extends OperatorRelationToStream{
	public DStreamOperator(JsonQueryTree tree, JStreamOutput outputStream) {
		super(tree, outputStream);
	}

	@Override
	protected void process(MarkedElement markedElement) {
		if(markedElement.mark == ElementMark.MINUS){
			markedElement.mark = ElementMark.UNMARKED;
			outputStream.pushNext(markedElement);
		}
	}
}
