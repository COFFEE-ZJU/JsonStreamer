package utils;

import java.util.HashMap;
import java.util.Map;

public class UserInfo {
	private Map<String, String> userNamePw;
	private UserInfo(){
		userNamePw = new HashMap<String, String>();
		userNamePw.put("user", "123123");
	}
	private static UserInfo ui = null;
	
	public boolean verify(String userName, String password){
		if(userNamePw.containsKey(userName) && userNamePw.get(userName).equals(password))
			return true;
		else return false;
	}
	
	public static UserInfo getInstance(){
		if(ui == null) ui = new UserInfo();
		return ui;
	}
}
