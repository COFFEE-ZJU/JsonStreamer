package json;

public class ElementIdGenerator {
	private ElementIdGenerator(){};
	private static long currentID = 0;
	public static long getNewId(){
		return currentID++;
	}
}
