package constants;

public class SyntaxErrorException extends JsonStreamerException{
	private static final long serialVersionUID = -3880815720524663931L;
	public SyntaxErrorException(){};
	public SyntaxErrorException(Exception e){
		super(e);
	};
	public SyntaxErrorException(String message){
		super(message);
	};
}
