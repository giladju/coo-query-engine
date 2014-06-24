package coo;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import scala.Tuple2;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

@EnableAutoConfiguration
@Controller
public class Sample implements Serializable {

	// JavaSparkContext sc = null;

	@RequestMapping("/")
	@ResponseBody
	public String helloWorld() {
		return "test1";
	}

	@RequestMapping("/geo")
	@ResponseBody
	public List<ClientRequest> loadGEO() throws IOException, TwitterException {
		List<String> readLines = FileUtils.readLines(new File(
				"C:\\data\\tweets_01.txt"));
		List<Status> statuses = new LinkedList<Status>();
		for (String line : readLines) {
			Status createStatus = TwitterObjectFactory.createStatus(line);
			statuses.add(createStatus);
		}

		SparkConf conf = new SparkConf().setAppName("Simple Application");
		conf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Status> parallelize = sc.parallelize(statuses);

		List<Status> statusList = parallelize.filter(
				new Function<Status, Boolean>() {
					@Override
					public Boolean call(Status v1) throws Exception {
						return v1.getGeoLocation() != null;
					}
				}).collect();

		List<ClientRequest> list = new LinkedList<ClientRequest>();

		for (Status status : statusList) {
			List<String> tags = new LinkedList<String>();
			if (status.getHashtagEntities() != null) {
				for (HashtagEntity hashtagEntity : status.getHashtagEntities()) {
					tags.add(hashtagEntity.getText());
				}
			}
			ClientRequest clientRequest = new ClientRequest(status.getId(),
					tags.toArray(new String[tags.size()]), status.getText(),
					status.getGeoLocation().getLatitude(), status
							.getGeoLocation().getLongitude());
			list.add(clientRequest);
		}
		return list;
	}

	@RequestMapping("/geoData")
	@ResponseBody
	public List<ClientRequest> loadGEOData(@RequestParam Query query)
			throws IOException, TwitterException {
		List<String> readLines = FileUtils.readLines(new File(
				"C:\\data\\tweets_01.txt"));
		List<Status> statuses = new LinkedList<Status>();
		for (String line : readLines) {
			Status createStatus = TwitterObjectFactory.createStatus(line);
			statuses.add(createStatus);
		}

		SparkConf conf = new SparkConf().setAppName("Simple Application");
		conf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Status> parallelize = sc.parallelize(statuses);

		List<Status> statusList = parallelize.filter(
				new Function<Status, Boolean>() {
					@Override
					public Boolean call(Status v1) throws Exception {
						if (v1.getGeoLocation() != null) {
							return true;
						} else {
							return false;
						}
					}
				}).collect();

		List<ClientRequest> list = new LinkedList<ClientRequest>();

		for (Status status : statusList) {
			List<String> tags = new LinkedList<String>();
			if (status.getHashtagEntities() != null) {
				for (HashtagEntity hashtagEntity : status.getHashtagEntities()) {
					tags.add(hashtagEntity.getText());
				}
			}
			ClientRequest clientRequest = new ClientRequest(status.getId(),
					tags.toArray(new String[tags.size()]), status.getText(),
					status.getGeoLocation().getLatitude(), status
							.getGeoLocation().getLongitude());
			list.add(clientRequest);
		}
		return list;
	}

	@RequestMapping("/geoCount")
	@ResponseBody
	public long loadGEOCount() throws IOException, TwitterException {
		List<String> readLines = FileUtils.readLines(new File(
				"C:\\data\\tweets_02.txt"));
		List<Status> statuses = new LinkedList<Status>();
		for (String line : readLines) {
			Status createStatus = TwitterObjectFactory.createStatus(line);
			statuses.add(createStatus);
		}

		SparkConf conf = new SparkConf().setAppName("Simple Application");
		conf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Status> parallelize = sc.parallelize(statuses);

		return parallelize.filter(new Function<Status, Boolean>() {
			@Override
			public Boolean call(Status v1) throws Exception {
				return v1.getGeoLocation() != null;
			}
		}).count();
	}

	@RequestMapping("/loadTwiter")
	@ResponseBody
	public String loadTwiter() throws IOException, TwitterException {
		List<String> readLines = FileUtils.readLines(new File(
				"C:\\data\\tweets_01.txt"));
		List<Status> statuses = new LinkedList<Status>();
		for (String line : readLines) {
			Status createStatus = TwitterObjectFactory.createStatus(line);
			statuses.add(createStatus);
		}

		SparkConf conf = new SparkConf().setAppName("Simple Application");
		conf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Status> parallelize = sc.parallelize(statuses);

		JavaRDD<String> flatMap = parallelize
				.flatMap(new FlatMapFunction<Status, String>() {
					@Override
					public Iterable<String> call(Status status)
							throws Exception {
						List<String> list = new LinkedList<String>();
						for (HashtagEntity hashTag : status
								.getHashtagEntities()) {
							list.add(hashTag.getText());
						}
						return list;
					}
				});

		JavaPairRDD<String, Integer> mapToPair = flatMap
				.mapToPair(new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String t)
							throws Exception {
						return new Tuple2<String, Integer>(t, 1);
					}
				});

		JavaPairRDD<String, Integer> reduceByKey = mapToPair
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						return v1 + v2;
					}
				});

		String debug = "";
		for (int i = 0; i < reduceByKey.count(); i++) {
			debug += reduceByKey.collect().get(i)._1 + ": "
					+ reduceByKey.collect().get(i)._2 + ".";
		}

		return debug;
	}

	@RequestMapping("/count")
	@ResponseBody
	public String count() throws IOException {

		String logFile = "C:\\dev\\workspace\\Tikal\\spark-1.0.0\\spark-1.0.0\\README.md"; // Should
																							// be
																							// some
																							// file
																							// on
		// your system
		// if (sc==null){
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		conf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// }
		JavaRDD<String> logData = sc.textFile(logFile).cache();
		long count = logData.count();
		long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("a");
			}
		}).count();

		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("b");
			}
		}).count();

		System.out.println("Lines with a: " + numAs + ", lines with b: "
				+ numBs);

		return String.format("test1, %d, %d", numAs, numBs);
	}

	@RequestMapping("/request")
	@ResponseBody
	public String requestion(ClientRequest clientRequest) {
		return "done";
	}

}
