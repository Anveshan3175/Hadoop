package com.anvesh.job;

import java.util.Arrays;

public class TestClass {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String[] tokens = "I am gonna Love you, Meghna".toString().split(",");
		System.out.println(Arrays.toString(tokens));
		System.out.println(tokens[1].trim());
	}

}
