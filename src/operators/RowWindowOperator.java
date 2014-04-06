package operators;

import jsonAPI.JsonQueryTree;

public class RowWindowOperator extends Operator{
	private final int rowSize;		//0 for now; -1 for unbounded
	public RowWindowOperator(JsonQueryTree tree){
		super(tree);
		rowSize = ((Double)tree.windowsize).intValue();
	}
	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}

}
