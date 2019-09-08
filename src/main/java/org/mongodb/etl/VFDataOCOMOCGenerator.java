package org.mongodb.etl;

import org.bson.Document;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Random;

// Contains a lot of copied code from VFDataGenerator
// probably these functions need to change to reflect the different data for OCOMC?
public class VFDataOCOMOCGenerator {

	private final int cpsIds;
	private Random rnd = new Random();
	private long ismi = 1L;

	public VFDataOCOMOCGenerator(int cpsIds) {
		this.cpsIds = cpsIds;
	}

	public Document createCSP(int i) {
		Document doc = new Document();
		doc.append("CSP_ID", String.format("%010d", rnd.nextInt(cpsIds)));
		doc.append("NBIOT_MESSAGE_SIZE", createNbiot());
		doc.append("THRESHOLDS", createThresholds());
		// right now only one bucket per CSP
		doc.append("TIMS", createTimsBucket(1+rnd.nextInt(99)));
//				System.out.println(doc);
		return doc;
	}

	private List<Document> createTimsBucket(int size) {
		List<Document> tims = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			Document doc = new Document();
			String id = String.format("%05d%010d", createIMSIPrefix(), getNextSequence());
			doc.append("id", id);
			int customer_id = rnd.nextInt(100000);
			doc.append("CUSTOMER_ID", customer_id);
			char state = (char) ('A' + rnd.nextInt(26));
			doc.append("STATE", state);
			String sim_identifier = id;
			doc.append("SIM_IDENTIFIER", sim_identifier);
			List<Document> thresholds = createThresholds();
			doc.append("THRESHOLDS", thresholds);
			List<Document> usages = createUsages();
			doc.append("USAGES", usages);
			tims.add(doc);
		}
		return tims;
	}

	private long getNextSequence() {
		return ismi++;
	}

	private List<Document> createUsages() {
		/**
		 *      "USAGE":[
		 *      	{"T":"PI",
		 *           "U":"2019-07-10T14:21:59.724000",
		 *           "H":[0,0,0,0,0,0,0,0,0,123,369,0,246,123,5,1476,369,0,246,0,0,0,0,0],
		 *           "D":[0,0,0,615,1107,0,0,0,0,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,369,861,0,0],
		 *           "M":[0,0,0,0,1107,1845,5,0,0,0,0,0],
		 *           "Y":2957,
		 *           "P":0},
		 *          {"T":"PC",
		 *           "U":"2019-07-10T14:21:59.724000",
		 *           "H":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
		 *           "D":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
		 *           "M":[0,0,0,0,0,0,0,0,0,0,0,0],
		 *           "Y":0,
		 *           "P":0},
		 *          {"T":"PO",
		 *           "U":"2019-07-10T14:21:59.724000",
		 *           "H":[0,0,0,0,0,0,0,0,0,456,1368,0,912,456,123,5472,1368,0,912,0,0,0,0,0],
		 *           "D":[0,0,0,2280,4104,0,0,0,0,123,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1368,3192,0,0],
		 *           "M":[0,0,0,0,4104,6840,123,0,0,0,0,0],
		 *           "Y":11067,
		 *           "P":0},
		 *          {"T":"PD",
		 *           "U":"2019-07-10T14:21:59.724000",
		 *           "H":[0,0,0,0,0,0,0,0,0,579,1737,0,1158,579,128,6948,1737,0,1158,0,0,0,0,0],
		 *           "D":[0,0,0,2895,5211,0,0,0,0,128,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1737,4053,0,0],
		 *           "M":[0,0,0,0,5211,8685,128,0,0,0,0,0],
		 *           "Y":14024,
		 *           "P":0},
		 *          {"T":"VD",
		 *           "U":"2019-06-28T16:00:57.493000",
		 *           "H":[0,0,0,0,0,0,0,0,0,0,0,0,4,0,0,0,1,0,0,0,0,0,0,0],
		 *           "D":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5,0,0,0],
		 *           "M":[0,0,0,0,0,5,0,0,0,0,0,0],
		 *           "Y":5,
		 *           "P":0},
		 *          {"T":"VC",
		 *           "U":"2019-06-28T16:00:57.493000",
		 *           "H":[0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,1,0,0,0,0,0,0,0],
		 *           "D":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,0,0],
		 *           "M":[0,0,0,0,0,2,0,0,0,0,0,0],
		 *           "Y":2,
		 *           "P":0}]
		 **/
		List<Document> usages = new ArrayList<Document>();
		usages.add(createUsage("PI"));
		usages.add(createUsage("PC"));
		usages.add(createUsage("PO"));
		usages.add(createUsage("PD"));
		usages.add(createUsage("VD"));
		usages.add(createUsage("VC"));
		return usages;
	}
	
	private Document createUsage(String type) {
		Document usage = new Document();
		usage.append("T", type);
		usage.append("U", createDate());
		usage.append("H", createArray(24, 10000));
		usage.append("D", createArray(31, 10000));
		usage.append("M", createArray(12, 10000));
		usage.append("Y", rnd.nextInt(10000));
		usage.append("P", 0);
		return usage;
	}
	
	private List<Integer> createArray(int size, int maxInt) {
		List<Integer> list = new ArrayList<Integer>();

		for (int i = 0; i < size; i++) {
			if (rnd.nextInt(10) == 0) {
				list.add(i, rnd.nextInt(maxInt));
			} else {
				list.add(i, 0);
			}
		}
		return list;
	}

	public int createIMSIPrefix() {
		int[] prefixArray = {94721, 3009, 98419, 68754, 51032, 91416, 79307, 21803, 90367, 14782};
		return prefixArray[rnd.nextInt(prefixArray.length)];
	}

	private long createNbiot() {
		int[] base = {8,9,10,11,12};
		long nbiotMessageSize = (long) Math.pow(2, base[rnd.nextInt(base.length)]);
		return nbiotMessageSize;
	}
	
	/**
	 * Fake thresholds, always serve the same for now
	 * @return a Document of thresholds
	 */
	private List<Document> createThresholds() {
		/**
		 *   "THRESHOLDS":[
		 *   	{"I":16339, "E":"U", "M":"PD", "L":"D", "P":"D", "T":"H", "V":17,
		 *       "C":"2019-08-08T05:08:53.939000","B":"2019-08-08T05:08:14.151000"}
		 *   ],
		 **/
		List<Document> thresholds = new ArrayList<Document>();
		Document threshold = new Document();
		threshold.append("I", rnd.nextInt(100000));
		threshold.append("E", "U");
		threshold.append("M", "PD");
		threshold.append("L", "D");
		threshold.append("P", "D");
		threshold.append("T", "H");
		threshold.append("V", 17);
		threshold.append("C", createDate());
		threshold.append("B", createDate());
		thresholds.add(threshold);
		return thresholds;
	}
	
	private String randomDateTime() throws ParseException {
		GregorianCalendar gc = new GregorianCalendar();
		int year = randBetween(2015, 2019);
		gc.set(GregorianCalendar.YEAR, year);
		int dayOfYear = randBetween(1, gc.getActualMaximum(GregorianCalendar.DAY_OF_YEAR));
		gc.set(GregorianCalendar.DAY_OF_YEAR, dayOfYear);

		//        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:SSS000");
		String date = gc.get(GregorianCalendar.YEAR) + "-" +
				gc.get(GregorianCalendar.MONTH) + "-" +
				gc.get(GregorianCalendar.DAY_OF_MONTH) + "T" +
				rnd.nextInt(24) + ":" +
				rnd.nextInt(60) + ":" + 
				rnd.nextInt(1000) + "000";
		return date; 
	}

	public static int randBetween(int start, int end) {
		return start + (int)Math.round(Math.random() * (end - start));
	}	

	/**
	 * Probably want to change that and do some checks, so it masks the randomDate()
	 * @return date String of format "yyyy-MM-dd'T'HH:mm:SSS000"
	 */
	private String createDate() {
		String date = "2018-01-01T00:00:000000";
		try {
			date = randomDateTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}

}
