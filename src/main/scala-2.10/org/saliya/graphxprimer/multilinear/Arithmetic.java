package org.saliya.graphxprimer.multilinear;

public interface Arithmetic<T> {
	
	public T add(T other);
	
	public T subtract(T other);
	
	public T multiply(T other);
	
	public T and(T other);
	
	public T or(T other);
	
	public T xor(T other);
	
	public T mod(T other);
	
	public T gcd(T other);
}