package operators;

import IO.JStreamOutput;

public class RootOperator  extends Operator{
	private JStreamOutput outputStream;
	
	public RootOperator(JStreamOutput stream){
		super(null);
		setOutputStream(stream);
	}
	
	public void setOutputStream(JStreamOutput stream){
		outputStream = stream;
	}
	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}

}
