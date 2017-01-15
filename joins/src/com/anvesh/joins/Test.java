package com.anvesh.joins;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String str = "1001|Alan|321";
		String[] arg = str.split("[|]");
		System.out.println(Arrays.toString(arg));
		List<String> vals = new ArrayList();
		vals.add("m_1001,Alan");
		vals.add("r_Economics");
		vals.add("m_1008,Arun");
		
		Iterator<String> it = vals.iterator();
		String temp = null;
		while(it.hasNext()){
			temp = it.next();
			if(temp.startsWith("m_"))
				System.out.println("m_"+temp);
		}
	}

}
