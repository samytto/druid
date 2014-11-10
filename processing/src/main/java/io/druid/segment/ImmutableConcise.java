package io.druid.segment;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import io.druid.segment.ImmutableBitmap;
import io.druid.segment.MutableBitmap;
import io.druid.segment.data.ConciseCompressedIndexedInts;
import io.druid.segment.data.ObjectStrategy;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

public class ImmutableConcise implements ImmutableBitmap {

	ImmutableConciseSet concise;
	
	public ImmutableConcise(){
		this.concise = new ImmutableConciseSet();
	}
	
	public ImmutableConcise(ImmutableConciseSet c){
		this.concise = c;
	}

	@Override
	public ObjectStrategy<Object> getObjectStrategy() {
		return (ObjectStrategy) ConciseCompressedIndexedInts.objectStrategy;
	}

	@Override
	public MutableBitmap getNewMutableBitmap() {
		return new MutableConcise();
	}

	@Override
	public ImmutableBitmap union(ImmutableBitmap b1, ImmutableBitmap b2) {
		ImmutableBitmap res = new ImmutableConcise(ImmutableConciseSet.union(((ImmutableConcise)b1).getConcise(), ((ImmutableConcise)b2).getConcise()));
		return res;
	}
	
	public ImmutableConciseSet getConcise(){
		return this.concise;
	}
}
