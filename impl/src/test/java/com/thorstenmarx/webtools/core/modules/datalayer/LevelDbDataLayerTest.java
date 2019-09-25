package com.thorstenmarx.webtools.core.modules.datalayer;

/*-
 * #%L
 * webtools-datalayer
 * %%
 * Copyright (C) 2016 - 2018 Thorsten Marx
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-3.0.html>.
 * #L%
 */
import com.thorstenmarx.webtools.api.datalayer.Expirable;
import com.thorstenmarx.webtools.api.datalayer.SegmentData;
import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Assertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 *
 * @author marx
 */
public class LevelDbDataLayerTest {

	private static final String TYPE = "atype";
	private static final String OTHER_TYPE = "otype";
	private static final String EMPTY_TYPE = "etype";

	private static final String NAME = "test name";

	LevelDBDataLayer layer;

	String id;

	@BeforeClass
	public void before() {
		layer = new LevelDBDataLayer(new File("./target/datalayer" + System.currentTimeMillis()));
		layer.open();
	}

	@AfterClass
	public void after() {
		layer.close();
	}

	@Test
	public void testAdd() {

		SegmentData data = new SegmentData();
		data.addSegment("eins", 1);
		data.addSegment("zwei", 2);

		layer.add("testAdd", "segments", data);

		Optional<List<SegmentData>> list = layer.list("testAdd", "segments", SegmentData.class);

		Assertions.assertThat(list).isPresent();
		final List<SegmentData> result = list.get();
		Assertions.assertThat(result).hasSize(1);
		Assertions.assertThat(result.get(0).getSegments()).isNotNull().isNotEmpty().containsExactlyInAnyOrder("eins", "zwei");
	}

	@Test
	public void testAdd_2() {

		SegmentData data = new SegmentData();
		data.addSegment("eins", 1);

		layer.add("testAdd", "segments2", data);

		data = new SegmentData();
		data.addSegment("zwei", 2);
		layer.add("testAdd", "segments2", data);

		Optional<List<SegmentData>> list = layer.list("testAdd", "segments2", SegmentData.class);

		Assertions.assertThat(list).isPresent();
		final List<SegmentData> result = list.get();
		Assertions.assertThat(result).hasSize(2);
	}

	@Test(expectedExceptions = UnsupportedOperationException.class)
	public void testUnmodifiable() {

		SegmentData data = new SegmentData();
		data.getSegments().add("eins");
	}

	@Test
	public void testMore() {

		SegmentData data = new SegmentData();
		data.addSegment("eins", 1);
		data.addSegment("zwei", 2);

		final long before = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++) {
			layer.add("testMore" + i, "segments", data);
		}
		final long after = System.currentTimeMillis();

		System.out.println("took: " + (after - before) + " ms");

	}

	@Test()
	public void testUpdateMulitpleTimes() {

		SegmentData data = new SegmentData();

		data.addSegment("eins", 1);

		layer.add("testUpdateMulitpleTimes", "segments", data);

		data.addSegment("zwei", 2);
		for (int i = 0; i < 1000; i++) {
			layer.add("testUpdateMulitpleTimes", "segments", data);
		}
	}

	@Test()
	public void test_clear() {

		SegmentData data = new SegmentData();
		data.addSegment("eins", 1);

		layer.add("testUpdateMulitpleTimes", "segments", data);

		data.addSegment("zwei", 2);
		for (int i = 0; i < 100; i++) {
			layer.add("testUpdateMulitpleTimes", "segments", data);
		}
		
		layer.clear("segments");
		
		layer.each((uid, sd) -> {
			Assertions.assertThat(true).isFalse().as("should never be called");
		}, "segments", SegmentData.class);
	}

}
