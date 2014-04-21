package IO;

import json.Element;
import json.JsonSchema;

public interface JStreamInput {
	//private JsonSchema schema;
	public Element getNextElement();
	public boolean isEmpty();
	public void execute();
}
