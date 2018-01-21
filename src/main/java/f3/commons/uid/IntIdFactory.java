/*
 * Copyright (c) 2010-2018 fork3
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES 
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE 
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR 
 * IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package f3.commons.uid;

import java.util.BitSet;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author n3k0nation
 *
 */
public class IntIdFactory implements IdFactory<Integer> {
	public final static int DEFAULT_CAPACITY = 100_000;
	public final static int DEFAULT_SHIFT = 0;
	
	private final IntIdFactoryContext ctx;
	private final AtomicInteger counter = new AtomicInteger();
	private final int threadNum;
	private final int shift;
	
	public IntIdFactory(int threadNum, int capacity, int shift) {
		if(capacity < 1) {
			throw new IllegalArgumentException("Capacity is zero or less.");
		}
		
		if(threadNum < 1) {
			throw new IllegalArgumentException("Thread number is zero or less.");
		}
		
		this.threadNum = threadNum;
		this.shift = shift;
		
		final int capacityPerThread = capacity / threadNum;
		final BitSet[] storage = new BitSet[threadNum];
		for(int i = 0; i < threadNum; i++) {
			storage[i] = new BitSet(capacityPerThread);
		}
		ctx = new IntIdFactoryContext(storage, capacity);
	}
	
	public IntIdFactory(int threadNum) {
		this(threadNum, DEFAULT_CAPACITY, DEFAULT_SHIFT);
	}
	
	@Override
	public void close(Integer id) {
		id -= shift;

		final int capacity = ctx.capacity;

		final float percent = (float) (capacity - id) / capacity;
		final int index;
		if(id == 0) {
			index = 1;
		} else {
			index = threadNum - (int) (percent * threadNum);
		}
		final BitSet bitset = ctx.storage[index - 1];
		final int capacityPerThread = capacity / threadNum;

		final int position = id > capacityPerThread ? index * capacityPerThread - id : id;
		synchronized (ctx.locks[index - 1]) {
			bitset.set(position);
			ctx.positions[index - 1] = 0;
		}
	}
	
	@Override
	public void close(Collection<Integer> ids) {
		for(int id : ids) {
			close(id);
		}
	}
	
	@Override
	public void free(Integer id) {
		id -= shift;
		
		final int capacity = ctx.capacity;

		final float percent = (float) (capacity - id) / capacity;
		final int index;
		if(id == 0) {
			index = 1;
		} else {
			index = threadNum - (int) (percent * threadNum);
		}
		final BitSet bitset = ctx.storage[index - 1];
		final int capacityPerThread = capacity / threadNum;

		final int position = id > capacityPerThread ? index * capacityPerThread - id : id;
		synchronized (ctx.locks[index - 1]) {
			bitset.clear(position);
			ctx.positions[index - 1] = position;
		}
	}
	
	@Override
	public Integer getId() {
		int position;
		
		final int capacityPerThread = ctx.capacity / threadNum;
		final int index = Math.abs(counter.getAndIncrement() % threadNum);
		final BitSet bitset = ctx.storage[index];
		synchronized (ctx.locks[index]) {
			position = bitset.nextClearBit(ctx.positions[index]);
			if(position >= capacityPerThread) {
				position = bitset.nextClearBit(0);
			}
	
			if (position >= capacityPerThread) {
				throw new RuntimeException("Id is out of range!");
			}
	
			bitset.set(position);
			ctx.positions[index] = position;
		}
		
		return index * capacityPerThread + position + shift;
	}
}
