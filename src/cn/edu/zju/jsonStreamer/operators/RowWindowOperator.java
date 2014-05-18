package cn.edu.zju.jsonStreamer.operators;

import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.parse.RegisterForLocal;

public class RowWindowOperator extends OperatorStreamToRelation{
	private final int rowSize;		//0 for now; -1 for unbounded
	
	public RowWindowOperator(JsonQueryTree tree, RegisterForLocal register, int opNum){
		super(tree, register, opNum);
		rowSize = ((Double)tree.windowsize).intValue();
	}

	@Override
	protected void process(MarkedElement markedElement) throws SystemErrorException {
		if(synopsis.size() == rowSize){
			MarkedElement eleToDelete = synopsis.poll();
			if(bufferedNum == rowSize) bufferedNum --;
			else
				output(new MarkedElement(eleToDelete.element, eleToDelete.id, ElementMark.MINUS, markedElement.timeStamp));
		}
		synopsis.add(markedElement);
		bufferedNum ++;
		output(markedElement);
		
	}

}
