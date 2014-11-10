package io.druid.segment;

import it.uniroma3.mat.extendedset.intset.ConciseSet;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class MutableConcise implements MutableBitmap {

	ConciseSet concise;
	
	public MutableConcise() {
		this.concise = new ConciseSet();
	}
	
	@Override
	public void add(int i) {
		this.concise.add(i);
	}

	@Override
	public ImmutableBitmap newImmutableFromMutable() {
		return new ImmutableConcise(ImmutableConciseSet.newImmutableFromMutable(concise));
	}

}
