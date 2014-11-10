package io.druid.segment;

import io.druid.segment.data.ObjectStrategy;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class MutableRoaring implements MutableBitmap {

	MutableRoaringBitmap roaring;
	
	public MutableRoaring() {
		this.roaring = new MutableRoaringBitmap();
	}
	
	@Override
	public void add(int i) {
		this.roaring.add(i);
	}

	@Override
	public ImmutableBitmap toNewImmutableFromMutable() {
		return new ImmutableRoaring((ImmutableRoaringBitmap)roaring);
	}
}
