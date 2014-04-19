package constants;

public class SystemErrorException extends RuntimeException{
	public SystemErrorException(){};
	public SystemErrorException(String message){
		super(message);
	};
}
