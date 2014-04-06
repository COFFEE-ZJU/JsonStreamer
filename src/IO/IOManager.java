package IO;

public class IOManager {
	private static IOManager ioManager = null;
	private IOManager(){}	//TODO
	
	public static IOManager getInstance(){
		if(ioManager == null) ioManager = new IOManager();
		
		return ioManager;
	}
	public JStreamOutput getOutputStreamByName(String streamName){
		return null;	//TODO
	}
	public JStreamInput getInputStreamByName(String streamName){
		return null;	//TODO
	}
}
