package constants;

public class SemanticErrorException extends RuntimeException{
	private static final long serialVersionUID = 214366107273386698L;
	public SemanticErrorException(){};
	public SemanticErrorException(String message){
		super(message);
	};
}
