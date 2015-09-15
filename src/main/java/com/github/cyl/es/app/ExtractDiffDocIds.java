package com.github.cyl.es.app;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class ExtractDiffDocIds {
	private static final String MONGO_HOST = "121.40.108.158";
	private static final int MONGO_PORT = 27017;
	private static final String DATABASE = "hugo_server_dtcj";
	private static final String COLLECTION = "documents";
	private static final String IDS_FILE = "d:/data/idList.txt";

	public static void main(String[] args) {
		MongoClient client = new MongoClient(MONGO_HOST, MONGO_PORT);
		MongoDatabase database = client.getDatabase(DATABASE);
		MongoCollection<Document> collection = database
				.getCollection(COLLECTION);

		int skip = 0;
		int totalNum = 500;
		List<Document> docList = new ArrayList<Document>();

		while (docList.size() < totalNum) {
			int fetchNum = totalNum - docList.size();
			FindIterable<Document> iterable = collection
					.find()
					.projection(
							new Document("_id", 1).append("title", 1).append(
									"origin_url", 1)).skip(skip)
					.limit(fetchNum);
			for (Document document : iterable) {
				if (!checkDuplicated(document, docList)) {
					docList.add(document);
				}
			}
			skip += fetchNum;
			System.out.println(docList.size());
		}
		client.close();

		List<String> idList = new ArrayList<String>();
		for (int i = 0; i < docList.size(); i++) {
			Document doc = docList.get(i);
			idList.add(doc.get("_id").toString());
		}

		writeStrListToTxt(idList, IDS_FILE);
	}

	private static boolean checkDuplicated(Document doc, List<Document> docList) {
		try {
			String title = doc.get("title").toString();
			String originUrl = doc.get("origin_url").toString();
			for (int i = 0; i < docList.size(); i++) {
				Document doc2 = docList.get(i);
				String title2 = doc2.get("title").toString();
				String originUrl2 = doc2.get("origin_url").toString();
				if (title.equals(title2) || originUrl.equals(originUrl2)) {
					return true;
				}
			}
			return false;
		} catch (Exception e) {
			return false;
		}
	}

	private static void writeStrListToTxt(List<String> list, String file) {
		try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(file), "utf-8"))) {
			for (int i = 0; i < list.size(); i++) {
				String str = list.get(i).trim();
				if (str != null && !str.isEmpty()) {
					bw.write(str);
					bw.newLine();
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
