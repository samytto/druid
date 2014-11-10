/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.data;

import com.google.common.collect.Ordering;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 */
public class RoaringCompressedIndexedInts implements IndexedInts, Comparable<RoaringCompressedIndexedInts>
{
  public static ObjectStrategy<ImmutableRoaringBitmap> objectStrategy =
      new ImmutableRoaringObjectStrategy();

  private static Ordering<ImmutableRoaringBitmap> comparator = new Ordering<ImmutableRoaringBitmap>()
  {
    @Override
    public int compare(
        @Nullable ImmutableRoaringBitmap Roaring, @Nullable ImmutableRoaringBitmap Roaring1
    )
    {
      if (Roaring.isEmpty() && Roaring1.isEmpty()) {
        return 0;
      }
      if (Roaring.isEmpty()) {
        return -1;
      }
      if (Roaring1.isEmpty()) {
        return 1;
      }
      
   // Do not know what the purpose of the comparator is, but
      // this is a comparison of sort... could be optimized
      // given a specification and a known purpose...
	IntIterator i1 = Roaring.getIntIterator();
	IntIterator i2 = Roaring1.getIntIterator();
	while (i1.hasNext() && i2.hasNext()) {
		  	int x1 = i1.next();
			int x2 = i2.next();
			if(x1 != x2) return x1 - x2; 
	}
	return i1.hasNext() ? 1 : (i2.hasNext() ? -1 : 0);
      
    }
  }.nullsFirst();

  private final ImmutableRoaringBitmap immutableRoaring;

  public RoaringCompressedIndexedInts(ImmutableRoaringBitmap Roaring)
  {
    this.immutableRoaring = Roaring;
  }

  @Override
  public int compareTo(RoaringCompressedIndexedInts roaringCompressedIndexedInts)
  {
	  // Do not know what the purpose of the comparator is, but
      // this is a comparison of sort... could be optimized
      // given a specification and a known purpose...
	IntIterator i1 = (IntIterator) immutableRoaring.getIntIterator();
	IntIterator i2 = (IntIterator) roaringCompressedIndexedInts.getImmutableRoaring().getIntIterator();
	while (i1.hasNext() && i2.hasNext()) {
		  	int x1 = i1.next();
			int x2 = i2.next();
			if(x1 != x2) return x1 - x2; 
	}
	return i1.hasNext() ? 1 : (i2.hasNext() ? -1 : 0);
  }

  @Override
  public int size()
  {
    return immutableRoaring.getCardinality();
  }

  @Override
  public int get(int index)
  {
    throw new UnsupportedOperationException("This is really slow, so it's just not supported.");
  }

  public ImmutableRoaringBitmap getImmutableRoaring()
  {
    return immutableRoaring;
  }

  @Override
  public Iterator<Integer> iterator()
  {
    return new Iterator<Integer>()
    {
      IntIterator baseIterator = immutableRoaring.getIntIterator();

      @Override
      public boolean hasNext()
      {
        return baseIterator.hasNext();
      }

      @Override
      public Integer next()
      {
        return baseIterator.next();
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static class ImmutableRoaringObjectStrategy implements ObjectStrategy<ImmutableRoaringBitmap>
  {
    @Override
    public Class<? extends ImmutableRoaringBitmap> getClazz()
    {
      return ImmutableRoaringBitmap.class;
    }

    @Override
    public ImmutableRoaringBitmap fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
    	final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
        
      return new ImmutableRoaringBitmap(readOnlyBuffer);
    }

    @Override
    public byte[] toBytes(ImmutableRoaringBitmap val)
    {
      if (val == null || val.isEmpty()) {
        return new byte[]{};
      }
      // converting to a byte array does not seem like a great
      // idea if you want to preserve memory... calling serialize
      // on a stream is better, but if you must generate an array
      // you certainly can:
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(bos);
try {
		val.serialize(dos);
		dos.close();
	} catch (IOException e) {e.printStackTrace();}
      
      return bos.toByteArray();
    }

    @Override
    public int compare(ImmutableRoaringBitmap o1, ImmutableRoaringBitmap o2)
    {
      return comparator.compare(o1, o2);
    }
  }

  public static void main(String[] args) {
	  
  }

}
