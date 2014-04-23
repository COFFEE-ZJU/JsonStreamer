package constants;

public class SystemErrorException extends JsonStreamerException{
	private static final long serialVersionUID = -7724129925329493700L;
	public SystemErrorException(){};
	public SystemErrorException(Exception e){
		super(e);
	};
	public SystemErrorException(String message){
		super(message);
	};
}
