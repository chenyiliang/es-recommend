package com.github.cyl.es.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Iterator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class ExAllIds {

	private static final String IN_PATH = "d:/data/rec_result.csv";
	private static final String OUT_PATH = "d:/data/raw_ids.txt";

	public static void main(String[] args) {
		File inFile = new File(IN_PATH);
		File outFile = new File(OUT_PATH);
		CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator('\n'); // 每条记录间隔符
		if (outFile.exists()) {
			outFile.delete();
		}
		Writer writer = null;
		Reader reader = null;
		BufferedWriter bw = null;
		CSVParser parser = null;
		try {
			writer = new OutputStreamWriter(new FileOutputStream(outFile),
					"utf-8");
			reader = new InputStreamReader(new FileInputStream(inFile), "utf-8");
			bw = new BufferedWriter(writer);
			parser = new CSVParser(reader, format);
		} catch (IOException e) {
			throw new RuntimeException("Csv File output preparing fails", e);
		}

		Iterator<CSVRecord> iterator = parser.iterator();
		while (iterator.hasNext()) {
			CSVRecord record = iterator.next();
			Iterator<String> iterator2 = record.iterator();
			while (iterator2.hasNext()) {
				String str = iterator2.next();
				// System.out.println(str);
				String id = str.split("\\|")[0];
				try {
					bw.write(id);
					bw.newLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			System.out.println("*********************");
		}

		try {
			bw.flush();
			parser.close();
			bw.close();
			writer.close();
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
