package lambda;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class S1Transform {

	String fileName = "";//"100 sales data.csv";
	String bucketName = "";//"tcss562-service1-group8";


	public String handleRequest(Request request, Context context) throws IOException {

		AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

		fileName = request.getFilename();
		bucketName = request.getBucketname();
		System.out.print("bucketName :: " + bucketName);
		System.out.print("fileName :: "  + fileName);
		try {
			S3Object s3object = s3.getObject(bucketName, fileName);
			displayTextInputStream(s3object.getObjectContent());
		}
		catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
		}
		String res = "File :: " + fileName + " Extracted and Transformed from  Bucket :: " +  bucketName;
		return res;
	}

	private void displayTextInputStream(InputStream input) throws IOException {
		// Read the text input stream one line at a time and display each line.
		List<ArrayList<String>> lines = new ArrayList<ArrayList<String>>();
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		String line = null;

		while ((line = reader.readLine()) != null) {

			String[] values = line.split(",");

			ArrayList<String> list = new ArrayList<String>();

			// Add the array to list
			Collections.addAll(list, values);
			lines.add(list);
		}
		transformData(lines);
	}

	public void transformData(List<ArrayList<String>> list){
		list = removeDuplicates(list);
//		try {
//			ListIterator<ArrayList<String>> iterator = list.listIterator();
			list = list.stream().map(row -> {

		try {
			int index = 0;
//			while (iterator.hasNext()) {
//				List<String> row = iterator.next();
//				For replacing the special charecters like single quote (')
			row = (ArrayList<String>) row.stream().map(x -> x.replaceAll("\'", " ")).collect(Collectors.toList());
			String element = row.get(4);
//			if (index == 0) {
				if ( row.contains("Region")) {
				row.add("Gross Margin");
				row.add("Order Processing Time");
//				index++;
			} else {
				float margin = Float.parseFloat(row.get(13)) / Float.parseFloat(row.get(11));
				row.add(String.valueOf((margin * 100)));

				SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy", Locale.ENGLISH);
				Date shipDate = sdf.parse(row.get(7));
				Date orderDate = sdf.parse(row.get(5));

				long days = (shipDate.getTime() - orderDate.getTime()) / (1000 * 60 * 60 * 24);
				row.add(String.valueOf(days));

				if (element.equals("L")) {
					row.set(4, "Low");
				} else if (element.equals("M")) {
					row.set(4, "Medium");
				} else if (element.equals("H")) {
					row.set(4, "High");
				} else if (element.equals("C")) {
					row.set(4, "Critical");
				}
			}
		} catch (Exception e) {
			System.out.println(e);
			return null;
		}
				return row;
		}).filter(x -> x != null).collect(Collectors.toList());
//		}
//		catch (Exception e) {
//			System.out.println(e);
//		}
		WriteCSV(list);
	}


	static void printCSV(List<ArrayList<String>> list){
		// the following code lets you iterate through the 2-dimensional array
		int lineNo = 1;
		for(List<String> line: list) {
			int columnNo = 1;
			for (String value: line) {
				System.out.println("Line " + lineNo + " Column " + columnNo + ": " + value);
				columnNo++;
			}
			lineNo++;
		}
	}

	public  List<ArrayList<String>> removeDuplicates(List<ArrayList<String>> list)
	{
		// Create a new ArrayList
		List<ArrayList<String>> newList = new ArrayList<ArrayList<String>>();

		// Traverse through the first list
		for (ArrayList<String> listIterator : list) {
			String element = listIterator.get(6);
			boolean isDuplicate = false;
			for (List<String> newlistIterator : newList) {
				String newelement = newlistIterator.get(6);
				if(element.equals(newelement)) isDuplicate = true;
			}
			if (!isDuplicate) {
				newList.add(listIterator);
			}
		}

		// return the new list
		return newList;
	}

	public void WriteCSV(List<ArrayList<String>> list){
		// the following code lets you iterate through the 2-dimensional array
		int lineNo = 1;
		StringWriter sw = new StringWriter();
		for(List<String> line: list) {
			int i = 0;
			for (String value: line) {
				sw.append(value);
				if(i++ != line.size() - 1)
					sw.append(',');
			}
			sw.append("\n");
			lineNo++;
		}

		byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
		InputStream is = new ByteArrayInputStream(bytes);
		ObjectMetadata meta = new ObjectMetadata();
		meta.setContentLength(bytes.length);

		// Create new file on S3
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
		s3Client.putObject(bucketName, "result.csv", is, meta);
	}

}