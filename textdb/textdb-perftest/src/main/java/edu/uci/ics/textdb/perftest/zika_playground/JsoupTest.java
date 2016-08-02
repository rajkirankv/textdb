package edu.uci.ics.textdb.perftest.zika_playground;

import java.io.File;
import java.util.Scanner;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class JsoupTest {
	
	public static void generatetxtfiles(String content) throws Exception {
		Document parsedDocument = Jsoup.parse(content);
		String wanted_info = parsedDocument.getElementById("preview").text();
		System.out.println(wanted_info);
	}
	
	
	
	public static void main(String[] args) throws Exception {
		
		Scanner scanner = new Scanner(new File("/Users/georgewang/Desktop/11111.txt"));
		StringBuilder sb = new StringBuilder();
		while (scanner.hasNextLine()) {
			sb.append(scanner.nextLine());
		}
		
		generatetxtfiles(sb.toString());
		
		scanner.close();
		
	}

}
