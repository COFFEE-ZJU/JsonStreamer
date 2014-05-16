package cn.edu.zju.jsonStreamer.operators;

import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;

public class RowWindowOperator extends OperatorStreamToRelation{
	private final int rowSize;		//0 for now; -1 for unbounded
	
	public RowWindowOperator(JsonQueryTree tree){
		super(tree);
		rowSize = ((Double)tree.windowsize).intValue();
	}

	@Override
	protected void process(MarkedElement markedElement) throws SystemErrorException {
		if(synopsis.size() == rowSize){
			MarkedElement eleToDelete = synopsis.poll();
			output(new MarkedElement(eleToDelete.element, eleToDelete.id, ElementMark.MINUS, markedElement.timeStamp));
		}
		synopsis.add(markedElement);
		output(markedElement);
		
	}

}
