package operators;

import constants.Constants.ElementMark;
import IO.JStreamOutput;
import json.MarkedElement;
import jsonAPI.JsonQueryTree;

public class IStreamOperator extends OperatorRelationToStream{
	
	public IStreamOperator(JsonQueryTree tree, JStreamOutput outputStream) {
		super(tree, outputStream);
	}

	@Override
	protected void process(MarkedElement markedElement) {
		if(markedElement.mark == ElementMark.PLUS){
			markedElement.mark = ElementMark.UNMARKED;
			outputStream.pushNext(markedElement);
		}
	}
}
