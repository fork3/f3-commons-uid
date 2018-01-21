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
package f3.commons.uid.test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import f3.commons.uid.IdFactory;
import f3.commons.uid.IntIdFactory;

/**
 * @author n3k0nation
 *
 */
public class IdFactoryTest {
	public final static int THREADS = 4;
	public final static int LIMIT = 50_000;
	
	public final static int CONCURENCY = LIMIT > 5_000 ? LIMIT / 5_000 : THREADS;
	
	private ExecutorService exec;
	private IdFactory<Integer> idFactory;
	
	@Before
	public void preload() {
		exec = Executors.newFixedThreadPool(THREADS);
		idFactory = new IntIdFactory(CONCURENCY, LIMIT, 0);
	}
	
	private List<Callable<Integer>> createTaskList(int size) {
		return createTaskList(size, () -> idFactory.getId());
	}
	
	private List<Callable<Integer>> createTaskList(int size, Callable<Integer> task) {
		return IntStream.range(0, size)
				.boxed()
				.map(i -> task)
				.collect(Collectors.toList());
	}
	
	@Test
	public void testDup() {
		final List<Callable<Integer>> list = createTaskList(LIMIT);
		
		final List<Integer> result;
		try {
			result = exec.invokeAll(list).stream()
					.map(f -> {
						try {
							return f.get();
						} catch(Throwable e) {
							e.printStackTrace();
							throw new RuntimeException(e);
						}
					})
					.collect(Collectors.toList());
		} catch (InterruptedException e) {
			e.printStackTrace();
			Assert.fail();
			return;
		}
		
		for(int i = 0; i < result.size(); i++) {
			final Integer n = result.get(i);
			long count = result.stream().filter(id -> id.intValue() == n.intValue()).count();
			Assert.assertEquals(String.format("Id '%d' has duplicates %d", n, count), 1, count);
		}
	}
	
	@Test
	public void testOverflow() {
		final List<Callable<Integer>> list = createTaskList(LIMIT + 1);
		
		boolean isCatchException = false;
		try {
			for(Future<Integer> f : exec.invokeAll(list)) {
				try {
					f.get();
				} catch(Throwable e) {
					isCatchException = true;
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			Assert.fail();
			return;
		}
		
		Assert.assertEquals(true, isCatchException);
	}
	
	@Test
	public void testFree() {
		int id = idFactory.getId();
		idFactory.free(id);
		System.out.println(String.format("Locked: %d", id));
		
		boolean passed = false;
		for(int i = 0; i < CONCURENCY << 1; i++) {
			final int nid = idFactory.getId();
			if(id == nid) {
				passed = true;
				break;
			}
		}
		
		Assert.assertEquals(true, passed);
	}
}
