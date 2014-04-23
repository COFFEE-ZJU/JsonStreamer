package constants;

public abstract class JsonStreamerException extends Exception{
	private static final long serialVersionUID = 7753920130128738773L;
	public JsonStreamerException(){};
	public JsonStreamerException(Exception e){
		super(e);
	};
	public JsonStreamerException(String message){
		super(message);
	};
}
