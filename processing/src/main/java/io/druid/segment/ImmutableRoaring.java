package io.druid.segment;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import io.druid.segment.ImmutableBitmap;
import io.druid.segment.MutableBitmap;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.data.RoaringCompressedIndexedInts;


public class ImmutableRoaring implements ImmutableBitmap {

	ImmutableRoaringBitmap roaring;
	
	public ImmutableRoaring(){
		this.roaring = new ImmutableRoaringBitmap(null);
	}

	public ImmutableRoaring(ImmutableRoaringBitmap r){
		this.roaring = r;
	}
	
	@Override
	public ObjectStrategy<Object> getObjectStrategy() {
		return (ObjectStrategy) RoaringCompressedIndexedInts.objectStrategy;
	}

	@Override
	public MutableBitmap getNewMutableBitmap() {
		return new MutableRoaring();
	}

	@Override
	public ImmutableBitmap union(ImmutableBitmap b1, ImmutableBitmap b2) {
		ImmutableBitmap res = new ImmutableRoaring(ImmutableRoaringBitmap.or(((ImmutableRoaring)b1).getRoaring(), ((ImmutableRoaring)b2).getRoaring()));
		return res;
	}
	
	public ImmutableRoaringBitmap getRoaring(){
		return this.roaring;
	}
}
