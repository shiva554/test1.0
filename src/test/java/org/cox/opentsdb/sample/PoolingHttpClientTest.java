package org.cox.opentsdb.sample;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.junit.Test;
import org.opentsdb.client.PoolingHttpClient;
import org.opentsdb.client.builder.MetricBuilder;
import org.opentsdb.client.response.SimpleHttpResponse;

public class PoolingHttpClientTest {

	@Test
	public void test_postJson_DefaultRetries() throws InterruptedException {
		PoolingHttpClient client = new PoolingHttpClient();

		Logger logger = Logger.getLogger("HttpClientTest");

	      String csvFile = "C:/Users/B33012/Desktop/COX/pnh_rtp_sbe_1516829101.csv";
			String line = "";
			String cvsSplitBy = ",";
			float latency ;
			int i = 0;
			MetricBuilder builder = MetricBuilder.getInstance();
			try {
				
				
				BufferedReader br = new BufferedReader(new FileReader(csvFile));
				

				while ((line = br.readLine()) != null && i<=120) {
					// use comma as separator
					String[] Column = line.split(cvsSplitBy);
					if (i <= 0) {
						builder.addMetric("performance.latency.test1").setDataPoint(1516828802, 0).addTag("src", Column[2]).addTag("service", Column[3])
								.addTag("path", Column[5]).addTag("dst", Column[8]);
						i++;
					} else {
						String src = Column[2].substring(0, 4);
						String Column3 = Column[3].replaceAll("\"", "");
						String service = Column3.substring(Column3.length() - 2).toLowerCase();
						String path = Column[5].substring(Column[5].length() - 5).toLowerCase();
						String dstn = Column[8].substring(0,4);
						String latency1 = Column[9];
						
						
						
						if (latency1.length()>0) {
							latency = Float.parseFloat(Column[9]);

							builder.addMetric("performance.latency.test1").setDataPoint(1516828802,latency).addTag("src", src)
									.addTag("service", service).addTag("path", path).addTag("dst", dstn);
						} else {
                           
							FileHandler logFile = new FileHandler("C:/Users/Public/Documents/abc.log", true);
							logger.addHandler(logFile);

							logger.setLevel(Level.WARNING);
							SimpleFormatter formatter = new SimpleFormatter();
							logFile.setFormatter(formatter);
							logger.warning("This test instanceid : " + Column[1] + " src : " + src + " dest : "
									+ dstn + " not pushed to openTSDB");
						}

					}i++;
					
					
				}
				
	


				SimpleHttpResponse response = client.doPost(
						"http://tsdb.coxsweng.com/api/put/?details",
						builder.build());
				System.out.println(response.getStatusCode());
				System.out.println(response.getContent());
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}