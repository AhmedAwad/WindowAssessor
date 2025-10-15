/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flink;

import flink.events.SimpleEvent;

import flink.source.YetAnotherSource;
import flink.triggers.EarlyCountEventTimeTrigger;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

import java.util.TimeZone;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your appliation into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJobTestWindowingSemantics {

	private static class AverageFunction extends RichMapFunction<Tuple2<Integer,Long>,String>
	{
		private transient ValueState<Tuple2<Long, Long>> countSumState;
		@Override
		public String map(Tuple2<Integer, Long> timeWindowLongTuple2) throws Exception {
			Tuple2<Long, Long> currentCountSum = countSumState.value();
			currentCountSum.f0+=1;
			currentCountSum.f1 +=timeWindowLongTuple2.f1;
			//System.out.println(String.format("Average so far is %f", (currentCountSum.f1.doubleValue())/currentCountSum.f0.longValue()));
			countSumState.update(currentCountSum);
			return String.format("Average so far is %f", ((double)currentCountSum.f1.longValue())/currentCountSum.f0.longValue());

		}

		@Override
		public void open(Configuration config)
		{
			ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
					new ValueStateDescriptor<Tuple2<Long, Long>>(
							"AverageNoElementsPerWindow",
							TypeInformation.of(
									new TypeHint<Tuple2<Long, Long>>() { }),
							Tuple2.of(0L,0L));

			countSumState = getRuntimeContext().getState(descriptor);
		}
	}
	private static class AverageTemperatureFunction implements AllWindowFunction<SimpleEvent, Tuple3<TimeWindow,Double,Integer>, TimeWindow>
    {
        @Override
        public void apply(TimeWindow window, Iterable<SimpleEvent> iterable, Collector<Tuple3<TimeWindow,Double,Integer>> collector) throws Exception {
            int count = 0;
            double sum = 0.0;
            for (SimpleEvent e : iterable)
            {
                count++;
                sum += e.getTemperature();
            }

            //System.out.println("Num elements in Window ("+window.getStart()+","+window.getEnd()+") is "+count);
            collector.collect(new Tuple3<>(window,Double.valueOf(sum/count), Integer.valueOf(count)));
        }
    }
	private static class CounterFunction implements AllWindowFunction <SimpleEvent, Tuple2<TimeWindow,Long>, TimeWindow>
	{


		@Override
		public void apply(TimeWindow window, Iterable<SimpleEvent> iterable, Collector<Tuple2<TimeWindow, Long>> collector) throws Exception {
			long count = 0L;
			for (SimpleEvent e : iterable)
			{
				count++;
			}

			//System.out.println("Num elements in Window ("+window.getStart()+","+window.getEnd()+") is "+count);
			collector.collect(new Tuple2<>(window, Long.valueOf(count)));
		}
	}

	private static class CounterFunction3 implements AllWindowFunction <Tuple3<Long,String,Double>, Tuple3<Long,Long,Long>, TimeWindow>
	{


		@Override
		public void apply(TimeWindow window, Iterable<Tuple3<Long,String,Double>> iterable, Collector<Tuple3<Long,Long, Long>> collector) throws Exception {
			long count = 0L;
			for (Tuple3<Long, String, Double> e : iterable)
			{
				count++;
			}
			SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
			sdfDate.setTimeZone(TimeZone.getTimeZone("GMT"));
			Date start, end;
			
			start =  new Date(window.getStart());
			end = new Date(window.getEnd());
			//System.out.println("Num elements in Window ("+window.getStart()+","+window.getEnd()+") is "+count);
			//collector.collect(new Tuple3<>(sdfDate.format(start),sdfDate.format(end), Long.valueOf(count)));
			collector.collect(new Tuple3<>(window.getStart(),window.getEnd(), count));
		}
	}

	private static class CounterFunction33 implements WindowFunction <Tuple3<Long,String,Double>, Tuple4<Long,Long,String,Long>, Tuple, TimeWindow>
	{


		@Override
		public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<Long,String,Double>> iterable, Collector<Tuple4<Long,Long,String,Long>> collector) throws Exception {
			long count = 0L;
			for (Tuple3<Long, String, Double> e : iterable)
			{
				count++;
			}
			SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
			sdfDate.setTimeZone(TimeZone.getTimeZone("GMT"));
//			Date start, end;
//
//			start =  new Date(window.getStart());
//			end = new Date(window.getEnd());
			//System.out.println("Num elements in Window ("+window.getStart()+","+window.getEnd()+") is "+count);
			//collector.collect(new Tuple3<>(sdfDate.format(start),sdfDate.format(end), Long.valueOf(count)));
			collector.collect(new Tuple4<>(window.getStart(),window.getEnd(), tuple.getField(0), count));
		}


	}
	private static class CounterFunction2 implements AllWindowFunction <SimpleEvent, Tuple2<GlobalWindow,Long>, GlobalWindow>
	{


		@Override
		public void apply(GlobalWindow window, Iterable<SimpleEvent> iterable, Collector<Tuple2<GlobalWindow, Long>> collector) throws Exception {
			long count = 0L;
			for (SimpleEvent e : iterable)
			{
				count++;
			}
//			System.out.println(window.toString());
			//System.out.println("Num elements in Window ("+window.getStart()+","+window.getEnd()+") is "+count);
			collector.collect(new Tuple2<>(window, Long.valueOf(count)));
		}
	}

    private static class CounterFunction22K implements WindowFunction<Tuple3<Long,String,Double>, Tuple4<String, String, String, Long>,Tuple, GlobalWindow>
    {


        @Override
        public void apply(Tuple  key, GlobalWindow window, Iterable<Tuple3<Long,String,Double>> iterable, Collector<Tuple4<String,String,String, Long>> collector) throws Exception {
            long count = 0L;
            SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
            sdfDate.setTimeZone(TimeZone.getTimeZone("GMT"));
            Long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;

            for (Tuple3<Long,String,Double> e : iterable)
            {
                min = Long.min(e.f0,min);
                max = Long.max(e.f0,max);
                count++;
            }
//			System.out.println(window.toString());
            //System.out.println("Num elements in Window ("+window.getStart()+","+window.getEnd()+") is "+count);
            Date start, end;
            start =  new Date(min);
            end = new Date(max);
            //System.out.println("Num elements in Window ("+window.getStart()+","+window.getEnd()+") is "+count);
            collector.collect(new Tuple4<>(sdfDate.format(start),sdfDate.format(end), key.toString(), Long.valueOf(count)));
        }


    }

	private static class CounterFunction22 implements AllWindowFunction <Tuple3<Long,String,Double>, Tuple3<String, String,Long>, GlobalWindow>
	{


		@Override
		public void apply(GlobalWindow window, Iterable<Tuple3<Long,String,Double>> iterable, Collector<Tuple3<String,String, Long>> collector) throws Exception {
			long count = 0L;
			SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
			sdfDate.setTimeZone(TimeZone.getTimeZone("GMT"));
			Long min = Long.MAX_VALUE;
			long max = Long.MIN_VALUE;

			for (Tuple3<Long,String,Double> e : iterable)
			{
				min = Long.min(e.f0,min);
				max = Long.max(e.f0,max);
				count++;
			}
//			System.out.println(window.toString());
			//System.out.println("Num elements in Window ("+window.getStart()+","+window.getEnd()+") is "+count);
			Date start, end;
			start =  new Date(min);
			end = new Date(max);
			//System.out.println("Num elements in Window ("+window.getStart()+","+window.getEnd()+") is "+count);
			collector.collect(new Tuple3<>(sdfDate.format(start),sdfDate.format(end), Long.valueOf(count)));
		}
	}
	static StreamExecutionEnvironment env;
	private static long slideWindowSize;
	private static long slideWindowSlide;
	private static long sessionWindowGap;
	private static long allowedLatness;
	public static void main(String[] args) throws Exception {


		String fileName;
//		fileName = "Data.csv";
		//fileName = "DataIntWithDelay.csv";
//		fileName = "DummyDataWithDelay.csv";
		fileName = "DummyDataWithDelaySession.csv";
//		fileName = "DummyDataInorderSession.csv";
		slideWindowSize=10;
		slideWindowSlide=2;
		sessionWindowGap = 10;
		allowedLatness=0;
//		jobForWindowTypeAndData(WindowType.Session,fileName, false,false, true);
//		jobForWindowTypeAndData(WindowType.SlidingTuple,fileName, false,false, true);
		jobForWindowTypeAndData(WindowType.Session,fileName, false, false, true);
	}



	private static DataStream<Tuple3<Long,String,Double>> getInputStream(String fileName)
	{
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		ExecutionConfig executionConfig = env.getConfig();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(0);
		DataStream<Tuple3<Long,String,Double>> stream = env.addSource(new YetAnotherSource(fileName));
		return stream;
	}

	private enum WindowType
	{
		SlidingTime,
		SlidingTuple,
		Session,
		Landmark
	}

	private static void jobForWindowTypeAndData(WindowType type, String fileName, boolean addEarlyTrigger, boolean addLateTrigger, boolean isPartitioned) throws Exception
	{
		// set up the streaming execution environment

        OutputTag<Tuple3<Long, String, Double>> lateItems= new OutputTag<Tuple3<Long, String, Double>>("Late Items"){};
 		DataStream<Tuple3<Long,String,Double>> stream = getInputStream(fileName);
        DataStream<Tuple3<Long,String,Double>> lateStream;
        String outputFileName = "Flink";
//		stream.timeWindowAll(Time.milliseconds(100), Time.milliseconds(10)).apply(new CounterFunction3()).writeAsText("forComparisonWithSpark.txt", FileSystem.WriteMode.OVERWRITE);
		if (type == WindowType.Session) {
			outputFileName+="SessionWindow";
			if (isPartitioned)
			{
				WindowedStream<Tuple3<Long, String, Double>, Tuple, TimeWindow> tuple3TimeWindowAllWindowedStream = stream.keyBy(1).window(EventTimeSessionWindows.
						withGap(Time.milliseconds(sessionWindowGap))).allowedLateness(Time.milliseconds(allowedLatness));
				if (addEarlyTrigger) {
//				tuple3TimeWindowAllWindowedStream.trigger(CountTrigger.of(2));
					tuple3TimeWindowAllWindowedStream.trigger(EarlyCountEventTimeTrigger.create(2));
					outputFileName+="WithEarlyTrigger";
				}
				if (addLateTrigger) {
					tuple3TimeWindowAllWindowedStream.sideOutputLateData(lateItems);
					outputFileName+="WithLateTrigger";
				}
				SingleOutputStreamOperator<Tuple4<Long, Long, String,Long>> streamOfResults = tuple3TimeWindowAllWindowedStream.apply(new CounterFunction33());
				if (addLateTrigger) {
					lateStream = streamOfResults.getSideOutput(lateItems);
					lateStream.countWindowAll(1).apply(new CounterFunction22()).writeAsText("FlinkSessionWindowLateItemsResult.txt", FileSystem.WriteMode.OVERWRITE);

				}
				streamOfResults.writeAsText(outputFileName+"For" + fileName + "Partitioned.txt", FileSystem.WriteMode.OVERWRITE);
			}
			else {
				AllWindowedStream<Tuple3<Long, String, Double>, TimeWindow> tuple3TimeWindowAllWindowedStream = stream.windowAll(EventTimeSessionWindows.withGap(Time.milliseconds(sessionWindowGap)))
						.allowedLateness(Time.milliseconds(allowedLatness));
				if (addEarlyTrigger) {
					//tuple3TimeWindowAllWindowedStream.trigger(CountTrigger.of(2));
					tuple3TimeWindowAllWindowedStream.trigger(EarlyCountEventTimeTrigger.create(2));
					outputFileName += "WithEarlyTrigger";
				}
				if (addLateTrigger) {
					tuple3TimeWindowAllWindowedStream.sideOutputLateData(lateItems);
					outputFileName += "WithLateTrigger";

				}
				SingleOutputStreamOperator<Tuple3<Long, Long, Long>> streamOfResults = tuple3TimeWindowAllWindowedStream.apply(new CounterFunction3());
				if (addLateTrigger) {
					lateStream = streamOfResults.getSideOutput(lateItems);
					lateStream.countWindowAll(1).apply(new CounterFunction22()).writeAsText("FlinkSessionWindowLateItemsResult.txt", FileSystem.WriteMode.OVERWRITE);

				}
				streamOfResults.writeAsText(outputFileName + "For" + fileName + ".txt", FileSystem.WriteMode.OVERWRITE);
			}
        }
		else if (type == WindowType.SlidingTime) {
            //stream.windowAll(SlidingEventTimeWindows.of(Time.milliseconds(100), Time.milliseconds(10))).apply(new CounterFunction3()).writeAsText("FlinkSlidingTimeWindowOutputFor" + fileName + ".txt", FileSystem.WriteMode.OVERWRITE);
			outputFileName+="SlidingTimeWindow";
			if (isPartitioned)
			{
				WindowedStream<Tuple3<Long, String, Double>, Tuple, TimeWindow> tuple3TimeWindowAllWindowedStream = stream.keyBy(1).
						window(SlidingEventTimeWindows.of(Time.milliseconds(slideWindowSize), Time.milliseconds(slideWindowSlide))).allowedLateness(Time.milliseconds(allowedLatness));
				if (addEarlyTrigger) {
//				tuple3TimeWindowAllWindowedStream.trigger(CountTrigger.of(2));
					tuple3TimeWindowAllWindowedStream.trigger(EarlyCountEventTimeTrigger.create(2));
					outputFileName+="WithEarlyTrigger";
				}
				if (addLateTrigger) {
					tuple3TimeWindowAllWindowedStream.sideOutputLateData(lateItems);
					outputFileName+="WithLateTrigger";
				}
				SingleOutputStreamOperator<Tuple4<Long, Long, String,Long>> streamOfResults = tuple3TimeWindowAllWindowedStream.apply(new CounterFunction33());
				if (addLateTrigger) {
					lateStream = streamOfResults.getSideOutput(lateItems);
					lateStream.countWindowAll(1).apply(new CounterFunction22()).writeAsText("FlinkSlidingTimeWindowLateItemsResult.txt", FileSystem.WriteMode.OVERWRITE);

				}
				streamOfResults.writeAsText(outputFileName+"For" + fileName + "Partitioned.txt", FileSystem.WriteMode.OVERWRITE);
			}
			else {
				AllWindowedStream<Tuple3<Long, String, Double>, TimeWindow> tuple3TimeWindowAllWindowedStream = stream.
						windowAll(SlidingEventTimeWindows.of(Time.milliseconds(slideWindowSize), Time.milliseconds(slideWindowSlide))).allowedLateness(Time.milliseconds(allowedLatness));
				if (addEarlyTrigger) {
//				tuple3TimeWindowAllWindowedStream.trigger(CountTrigger.of(2));
					tuple3TimeWindowAllWindowedStream.trigger(EarlyCountEventTimeTrigger.create(2));
					outputFileName+="WithEarlyTrigger";
				}
				if (addLateTrigger) {
					tuple3TimeWindowAllWindowedStream.sideOutputLateData(lateItems);
					outputFileName+="WithLateTrigger";
				}
				SingleOutputStreamOperator<Tuple3<Long, Long, Long>> streamOfResults = tuple3TimeWindowAllWindowedStream.apply(new CounterFunction3());
				if (addLateTrigger) {
					lateStream = streamOfResults.getSideOutput(lateItems);
					lateStream.countWindowAll(1).apply(new CounterFunction22()).writeAsText("FlinkSlidingTimeWindowLateItemsResult.txt", FileSystem.WriteMode.OVERWRITE);

				}
				streamOfResults.writeAsText(outputFileName+"For" + fileName + ".txt", FileSystem.WriteMode.OVERWRITE);
			}
        }
		else if (type == WindowType.SlidingTuple) {
		    // There is no meaning for late arrival in count windows
            if (isPartitioned) {

                stream.keyBy(1).countWindow(5,2).apply( new CounterFunction22K()).writeAsText("FlinkSlidingTupleWindowPartitionedOutputFor" + fileName + ".txt", FileSystem.WriteMode.OVERWRITE);
            }
            else
                stream.countWindowAll(5, 2).apply(new CounterFunction22()).writeAsText("FlinkSlidingTupleWindowOutputFor" + fileName + ".txt", FileSystem.WriteMode.OVERWRITE);
        }
		env.execute("Flink Job for Session Window on In-order Data");
	}




}
