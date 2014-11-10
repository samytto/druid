package io.druid.segment;

public interface MutableBitmap {

	public void add(int i);
	
	public ImmutableBitmap toNewImmutableFromMutable();
		
}
