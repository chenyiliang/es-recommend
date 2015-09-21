package com.github.cyl.es.test;

import org.apdplat.word.analysis.CosineTextSimilarity;
import org.apdplat.word.analysis.TextSimilarity;

public class TextSimilarityTest {

	public static void main(String[] args) {
		String text1 = "我爱学习";
		String text2 = "我爱读书";
		String text3 = "他是黑客";
		TextSimilarity textSimilarity = new CosineTextSimilarity();
		double score1pk1 = textSimilarity.similarScore(text1, "我" + text1);
		double score1pk2 = textSimilarity.similarScore(text1, text2);
		double score1pk3 = textSimilarity.similarScore(text1, text3);
		double score2pk2 = textSimilarity.similarScore(text2, text2);
		double score2pk3 = textSimilarity.similarScore(text2, text3);
		double score3pk3 = textSimilarity.similarScore(text3, text3);
		System.out.println(text1 + " 和 " + text1 + " 的相似度分值：" + score1pk1);
		System.out.println(text1 + " 和 " + text2 + " 的相似度分值：" + score1pk2);
		System.out.println(text1 + " 和 " + text3 + " 的相似度分值：" + score1pk3);
		System.out.println(text2 + " 和 " + text2 + " 的相似度分值：" + score2pk2);
		System.out.println(text2 + " 和 " + text3 + " 的相似度分值：" + score2pk3);
		System.out.println(text3 + " 和 " + text3 + " 的相似度分值：" + score3pk3);
	}

}
