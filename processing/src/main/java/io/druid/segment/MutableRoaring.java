package io.druid.segment;

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
		// TODO Auto-generated method stub
		return null;
	}

}
