package io.druid.segment;

import io.druid.segment.data.ObjectStrategy;

public interface ImmutableBitmap {
	
	public ObjectStrategy<Object> getObjectStrategy();
	
	public MutableBitmap getNewMutableBitmap();
	
	public ImmutableBitmap union(ImmutableBitmap b1, ImmutableBitmap b2);
	
}
