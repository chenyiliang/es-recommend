package com.github.cyl.es.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class PairIdToMongo {
	private static final String MONGO_HOST = "121.40.108.158";
	private static final int MONGO_PORT = 27017;
	private static final String DATABASE = "recommend";
	private static final String COLLECTION = "recpair";
	private static final String IN_PATH = "d:/data/rec_result.csv";

	public static void main(String[] args) {
		// 要导入数据库的文档
		List<Document> docList = new ArrayList<Document>();
		File inFile = new File(IN_PATH);
		CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator('\n'); // 每条记录间隔符
		if (!inFile.exists()) {
			throw new RuntimeException("原始输入文件不存在！");
		}

		Reader reader = null;
		CSVParser parser = null;
		try {
			reader = new InputStreamReader(new FileInputStream(inFile), "utf-8");
			parser = new CSVParser(reader, format);
		} catch (IOException e) {
			throw new RuntimeException("Csv File output preparing fails", e);
		}

		Iterator<CSVRecord> iterator = parser.iterator();
		while (iterator.hasNext()) {
			CSVRecord record = iterator.next();
			Iterator<String> iterator2 = record.iterator();
			boolean isTarget = true;
			String target = null;
			while (iterator2.hasNext()) {
				if (isTarget) {
					target = iterator2.next();
					isTarget = false;
					continue;
				}
				String[] recAndScore = iterator2.next().trim().split("\\|");
				if (recAndScore.length == 2) {
					String rec = recAndScore[0];
					String score = recAndScore[1];

					Map<String, Object> map = new HashMap<String, Object>();
					map.put("target", target);
					map.put("rec", rec);
					map.put("score", Float.valueOf(score));
					docList.add(new Document(map));
				}
			}
		}

		System.out.println(docList.size());

		try {
			parser.close();
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		importToMongoDB(MONGO_HOST, MONGO_PORT, DATABASE, COLLECTION, docList);
	}

	private static void importToMongoDB(String host, int port,
			String databaseName, String collectionName, List<Document> docList) {
		MongoClient client = new MongoClient(host, port);
		MongoDatabase database = client.getDatabase(databaseName);
		MongoCollection<Document> collection = database
				.getCollection(collectionName);

		collection.insertMany(docList);

		client.close();
	}
}
