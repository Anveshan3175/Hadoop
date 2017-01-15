package com.Inner;

public class TestInner {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Add aAdd = new Add() {
			@Override
			public int sum(int a, int b) {
				return a + b;
			}
		};
		System.out.println(aAdd.sum(12, 45));
	}
}

interface Add {
	int sum(int a, int b);
}