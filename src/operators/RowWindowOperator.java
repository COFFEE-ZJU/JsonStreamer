package operators;

import constants.Constants.ElementMark;

import json.MarkedElement;
import jsonAPI.JsonQueryTree;

public class RowWindowOperator extends OperatorStreamToRelation{
	private final int rowSize;		//0 for now; -1 for unbounded
	
	public RowWindowOperator(JsonQueryTree tree){
		super(tree);
		rowSize = ((Double)tree.windowsize).intValue();
	}

	@Override
	protected void process(MarkedElement markedElement) {
		if(synopsis.size() == rowSize){
			MarkedElement eleToDelete = synopsis.poll();
			outputQueue.add(new MarkedElement(eleToDelete.element, eleToDelete.id, ElementMark.MINUS, markedElement.timeStamp));
		}
		synopsis.add(markedElement);
		outputQueue.add(markedElement);
		
	}

}
