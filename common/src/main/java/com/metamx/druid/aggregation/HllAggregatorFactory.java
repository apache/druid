package com.metamx.druid.aggregation;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.metamx.druid.processing.ColumnSelectorFactory;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.logger.Logger;
import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.map.TIntByteMap;

public class HllAggregatorFactory implements AggregatorFactory {

	private static final byte CACHE_TYPE_ID = 0x2;

	private final String fieldName;
	private final String name;
	private static final Logger log = new Logger(HllAggregatorFactory.class);
	
	static final int NUMARR = 7;
	static final int ARRLEN = 41;
	public static final int HLL_LOBINS = 1024;
	public static final int HLL_HIBINS = 65536;
 public static enum CONTEXT{
         ORIGNAL,
         COMPLEX
}
	public static CONTEXT context = CONTEXT.ORIGNAL;
	
	private static final double[][] RARR = {
		//r1024
			{1.346489469602470E+03, 1.378712298083080E+03, 1.414092791426060E+03, 
			1.448645346545680E+03, 1.488104867991980E+03, 1.528666394318380E+03, 
			1.570379254605160E+03, 1.614622644105020E+03, 1.663519088253900E+03, 
			1.715206108597840E+03, 1.762776595177530E+03, 1.822975686794980E+03, 
			1.886103333764340E+03, 1.946204430704690E+03, 2.014960037941740E+03, 
			2.093516436720130E+03, 2.162265735371530E+03, 2.240805343742080E+03, 
			2.328372479917740E+03, 2.411562196780970E+03, 2.508818438577450E+03, 
			2.604651621730100E+03, 2.711374662624540E+03, 2.819966864735790E+03, 
			2.940859521997580E+03, 3.063948271356980E+03, 3.179334208371330E+03, 
			3.320541165305440E+03, 3.459837521374780E+03, 3.616677888314370E+03, 
			3.772022026694140E+03, 3.932060610568670E+03, 4.101365315737600E+03,
			4.285717957855780E+03, 4.477469695127110E+03, 4.670488107127940E+03,
			4.864707621697200E+03, 5.094769028211500E+03, 5.303859800032660E+03,
			5.539352439151380E+03, 5.792618751480197E+03}, //5.793318718839120E+03
		
		//r2048 =
			{2.692406872188190E+03, 2.758018954120040E+03, 2.826851114243140E+03, 
			2.899400176533320E+03, 2.978536486853930E+03, 3.053733907595210E+03, 
			3.143095385393840E+03, 3.232545743545010E+03, 3.327206148131450E+03, 
			3.425383621935400E+03, 3.535113113233790E+03, 3.651380734979320E+03, 
			3.772604812214650E+03, 3.891803065600730E+03, 4.028046624690940E+03, 
			4.167290013485090E+03, 4.326239707080610E+03, 4.483893133168400E+03, 
			4.649830866092060E+03, 4.826501555459110E+03, 5.018949280343620E+03, 
			5.222252900953780E+03, 5.419875620271040E+03, 5.638594184590650E+03, 
			5.880021717092490E+03, 6.117206957516370E+03, 6.380951194140120E+03, 
			6.642435277024590E+03, 6.924104749173030E+03, 7.218018749640280E+03, 
			7.537972902856540E+03, 7.853292575569320E+03, 8.211302138007860E+03,
			8.582526513369940E+03, 8.942864929365450E+03, 9.338112044250970E+03,
			9.754190639187930E+03, 1.018123110322760E+04, 1.063829537376500E+04,
			1.109152041661780E+04, 1.1585237502960394E+04}, //1.158252141591760E+04
		
		//r4096 =
			{5.389565277158610E+03, 5.517687268603150E+03, 5.658055280492120E+03, 
			5.797932731546300E+03, 5.948989504238240E+03, 6.114439958479500E+03, 
			6.281092112639760E+03, 6.460483912010050E+03, 6.653229011985890E+03, 
			6.859889870680140E+03, 7.069827243654020E+03, 7.300562438532550E+03, 
			7.532767806188190E+03, 7.793388841206350E+03, 8.054494839991950E+03, 
			8.340003438269290E+03, 8.645390783677310E+03, 8.965147982409270E+03, 
			9.294234936256430E+03, 9.660807366678510E+03, 1.003155852517900E+04, 
			1.042841234625580E+04, 1.084552275943100E+04, 1.127799477110740E+04, 
			1.175004683737310E+04, 1.223373680895290E+04, 1.273903068303720E+04, 
			1.328342575883440E+04, 1.384923823685580E+04, 1.445411507423130E+04, 
			1.508048818012620E+04, 1.573115811850550E+04, 1.640575357912070E+04, 
			1.709989763788940E+04, 1.789996654687340E+04, 1.867575550004890E+04, 
			1.949096731053140E+04, 2.033570851413750E+04, 2.123056910218420E+04, 
			2.217943318815900E+04, 2.3170475005920787E+04}, //2.316004994337210E+04
		
		//r8192 =
			{1.078241387541600E+04, 1.104403373234450E+04, 1.130910290023750E+04, 
			1.159519982786630E+04, 1.190071022061370E+04, 1.222046880545390E+04, 
			1.256825151904770E+04, 1.292917807823150E+04, 1.331372864646630E+04, 
			1.371634932297990E+04, 1.413912848804380E+04, 1.459288531095730E+04, 
			1.507878331973030E+04, 1.558984699785480E+04, 1.611880727108750E+04, 
			1.669905294740690E+04, 1.729275986856990E+04, 1.792760787973680E+04, 
			1.860000733256200E+04, 1.929806694902870E+04, 2.006647923890300E+04, 
			2.085075540342150E+04, 2.169275423739040E+04, 2.255278563029190E+04, 
			2.348031621071180E+04, 2.447084346154250E+04, 2.551193418145200E+04, 
			2.657900777056680E+04, 2.770247081789330E+04, 2.888240463373810E+04, 
			3.016996325195980E+04, 3.146532794886390E+04, 3.280857355176500E+04,
			3.424593777524430E+04, 3.576576637148320E+04, 3.731295024506480E+04,
			3.903466654157440E+04, 4.069064523322140E+04, 4.251652150217010E+04,
			4.438260047868100E+04, 4.6340950011841574E+04}, //4.636050404383660E+04
		
		//r16384 =
			{2.155906149609910E+04, 2.207096985518480E+04, 2.262919638851020E+04, 
			2.319798103685170E+04, 2.380288452091340E+04, 2.445378529255160E+04, 
			2.513377927819550E+04, 2.585507608646100E+04, 2.661669578021650E+04, 
			2.743764158595520E+04, 2.829443398208400E+04, 2.918497885121380E+04, 
			3.014534426764810E+04, 3.117050042197490E+04, 3.225289064736110E+04, 
			3.338760761113710E+04, 3.458546170121470E+04, 3.586286034056040E+04, 
			3.719898716012420E+04, 3.861622297267060E+04, 4.011265100463560E+04, 
			4.170375272952180E+04, 4.339916626854950E+04, 4.515499737396580E+04, 
			4.699405066592360E+04, 4.895052436626750E+04, 5.097578473524260E+04, 
			5.314216307523730E+04, 5.540474006042170E+04, 5.779927327902710E+04, 
			6.030188142926680E+04, 6.295522468536780E+04, 6.565359335342100E+04,
			6.854443273944810E+04, 7.151581938634910E+04, 7.465908687528500E+04,
			7.797712638873120E+04, 8.143088558427540E+04, 8.499646382417170E+04,
			8.878985502991190E+04, 9.268190002368315E+04}, //9.267779762293240E+04
		
		//r32768 =
			{4.313313206382800E+04, 4.415269834455130E+04, 4.524150083599280E+04, 
			4.639131523355750E+04, 4.762090051765090E+04, 4.890155451482830E+04, 
			5.027809939367190E+04, 5.170999836012450E+04, 5.323363783317260E+04, 
			5.485776193689410E+04, 5.657179481734920E+04, 5.839171981365430E+04, 
			6.030102327942410E+04, 6.234377653076620E+04, 6.449053358726530E+04, 
			6.678150343981330E+04, 6.915864767783630E+04, 7.172752283050760E+04, 
			7.440261847855430E+04, 7.723279369942860E+04, 8.024557464998870E+04, 
			8.340741528491580E+04, 8.673867489877550E+04, 9.026845820894900E+04, 
			9.397192634086610E+04, 9.787513826541650E+04, 1.019718064255530E+05, 
			1.063260494685030E+05, 1.108226110217690E+05, 1.155730191017230E+05, 
			1.205985168572430E+05, 1.258284362310310E+05, 1.312645880322270E+05,
			1.370778534311660E+05, 1.430875481221300E+05, 1.493408146036170E+05,
			1.559607667422240E+05, 1.628029300631030E+05, 1.700693186624480E+05,
			1.775506935853310E+05, 1.853638000473663E+05}, //1.853638088467130E+05
		
		//r65536 =
			{8.625612827433010E+04, 8.830595325118660E+04, 9.048718527933750E+04, 
			9.279590169003630E+04, 9.523365507191660E+04, 9.781265347247010E+04, 
			1.005382040769500E+05, 1.034112319251390E+05, 1.064912717027580E+05, 
			1.097180266326620E+05, 1.131368251501060E+05, 1.167848184091800E+05, 
			1.206271726784100E+05, 1.246897543981110E+05, 1.289661841114660E+05, 
			1.335337037409350E+05, 1.383328031824140E+05, 1.434152101610130E+05, 
			1.488163901654310E+05, 1.544919496608530E+05, 1.604746524962890E+05, 
			1.668044458283640E+05, 1.734856397395560E+05, 1.805294000422780E+05, 
			1.879299511926260E+05, 1.957469740791460E+05, 2.039604121990220E+05, 
			2.125950730047330E+05, 2.217038549355720E+05, 2.311733976684120E+05, 
			2.411374455890950E+05, 2.516489657699140E+05, 2.626409977008660E+05, 
			2.740715187509110E+05, 2.860959989847380E+05, 2.987147428447790E+05, 
			3.118784298578110E+05, 3.256590892529460E+05, 3.400521726492160E+05, 
			3.549685482916180E+05, 3.707276000947326E+05} //3.707313223830110E+05
		};

		static double[][] getRegressionArrays() {
			return RARR.clone();
		}
		
		/**
		 * <p>Map the given <i>hllEst</i> (the output of the HLL algorithm) to a 
		 * corrected cardinality value. If the <i>hllEst</i> value is below the 
		 * range of values for the selected <i>bins</i> curve, zero will be returned. 
		 * If it is above the range it will return the given <i>hllEst</i> value.</p>
		 * 
		 * <p>
		 * @param bins
		 * @param hllEst
		 */
		public static double regress(int bins, double hllEst) {
			int arrIdx = check(bins);
			double[] arr = RARR[arrIdx];
			double lo = arr[0]; //lo, hi defined for bins dimension
			double hi = arr[ARRLEN-1];
			if (hllEst < lo) return 0; //signal below range
			if (hllEst >= hi) return hllEst;//signal above range
			//linear guess "skip" search
			int i = (int)Math.floor((hllEst - lo)/(hi-lo));
			//if not right on it should be one below or one above 
			while (hllEst < arr[i]) i--;    //below the lower bound?
			while (hllEst >= arr[i+1]) i++; //above the upper bound?
			lo = arr[i];  //lo, hi now defined for the selected interior segment
			hi = arr[i+1];
			//simple linear regression within segment.
			double f = (hllEst - lo)/(hi-lo); 
			double r1 = bins * Math.pow(2.0, i/16.0);
			double r2 = bins * Math.pow(2.0, (i+1)/16.0);
			return r1 + f * (r2-r1);
		}
		
		/**
		 * Check that given <i>bins</i> parameter is a positive integer power of 2
		 * and within the range of provided regression curves.
		 * @param bins The configured <i>bins</i> value used for the 
		 * CountUniqueHLLSketch class.
		 */
		private static int check(int bins) {
			checkIfPowerOf2(bins, "bins");
			checkBounds(bins, HLL_LOBINS, HLL_HIBINS, "Bins");
			int lobinsExp = Integer.numberOfTrailingZeros(HLL_LOBINS);
			int binsExp = Integer.numberOfTrailingZeros(bins);
			return binsExp - lobinsExp;
		}
		public static void checkIfPowerOf2(final int v, final String argName) {
	        if ( (v > 0) && ((v & (v-1)) ==0) )  return;
	        throw new IllegalArgumentException(
	            "The value of the parameter \""+argName+
	            "\" must be a positive integer-power of 2"+
	            " and greater than 0: "+v);
	    }
		public static void checkBounds(final int v, 
		    	final int minValue, final int maxValue, final String name) {
		    	if ( (v >= minValue) && (v <= maxValue)) return;
		    	throw new IllegalArgumentException(
		    		name+" must be >= "+minValue+" and <= "+maxValue+": "+v);
		    }
			
	@JsonCreator
	public HllAggregatorFactory(@JsonProperty("name") String name,
			@JsonProperty("fieldName") final String fieldName) {
		Preconditions.checkNotNull(name,
				"Must have a valid, non-null aggregator name");
		Preconditions.checkNotNull(fieldName,
				"Must have a valid, non-null fieldName");

		this.name = name;
		this.fieldName = fieldName;
	}

	@Override
	public Aggregator factorize(ColumnSelectorFactory metricFactory) {
		log.info("HllAggregatorFactory factorize");
		return new HllAggregator(name,
				metricFactory.makeObjectColumnSelector(fieldName));
	}

	@Override
	public BufferAggregator factorizeBuffered(
			ColumnSelectorFactory metricFactory) {
		log.info("HllAggregatorFactory factorize buffer {}, class: {}",
				fieldName, metricFactory.getClass().getName());
		// TODO Auto-generated method stub
		return new HllBufferAggregator(
				metricFactory.makeObjectColumnSelector(fieldName));
	}

	@Override
	public Comparator getComparator() {
		// TODO Auto-generated method stub
		log.info("HllAggregatorFactory comparator");
		return HllAggregator.COMPARATOR;
	}

	@Override
	public Object combine(Object lhs, Object rhs) {
		log.info("HllAggregatorFactory combine");
		// TODO Auto-generated method stub
		return HllAggregator.combineHll(lhs, rhs);
	}

	@Override
	public AggregatorFactory getCombiningFactory() {
		log.info("HllAggregatorFactory getCombiningFactory");
		for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
			log.info("HllAggregatorFactory " + ste);
		}
		// TODO Auto-generated method stub
		return new HllAggregatorFactory(name, name);
	}

	@Override
	public Object deserialize(Object object) {
		log.info("HllAggregatorFactory deserialize");
		// TODO Auto-generated method stub
		return object;
	}

	@Override
	public Object finalizeComputation(Object object) {
		TIntByteHashMap ibMap = (TIntByteHashMap) object;
		int[] keys = ibMap.keys();
		double registerSum = 0;
		int count = keys.length;
		double zeros = 0.0;
		log.info("kyes {}, values {}", keys, ibMap.values());
		for (int i = 0; i < keys.length; i++) {
			{
				log.info("key: {%s}, |||values: {%s}", keys[i],
						ibMap.get(keys[i]));
				int val = ibMap.get(keys[i]);
				registerSum += 1.0 / (1 << val);
				if (val == 0) {
					zeros++;
				}
			}

		}
		registerSum += (HllAggregator.m - count);
		zeros += HllAggregator.m - count;

		log.info("registerSum:" + registerSum + "|||zeros:" + zeros);
		double estimate = HllAggregator.alphaMM * (1.0 / registerSum);

		double regressEst = regress(HllAggregator.m, estimate);
		        if (regressEst > 0.0){
		               return regressEst;
		        }
		
		if (estimate <= (5.0 / 2.0) * (HllAggregator.m)) {
			// Small Range Estimate
			log.info("final small value:");
			return Math.round(HllAggregator.m
					* Math.log(HllAggregator.m / zeros));
		} else {
			log.info("final big value:" + Math.round(estimate));
			return Math.round(estimate);
		}

		// for (int i = 0; i < a.length; i++) {
		// // log.info("final value:" + a[i] + "|||| index:" + i);
		// }
		// log.info("HllAggregatorFactory finalizeComputation" + new
		// Exception());
		// TODO Auto-generated method stub
		// return object;
	}

	@JsonProperty
	public String getFieldName() {
		log.info("HllAggregatorFactory getFieldName");
		return fieldName;
	}

	@Override
	@JsonProperty
	public String getName() {
		log.info("HllAggregatorFactory getName" + new Exception());
		for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
			log.info("HllAggregatorFactory getName" + ste);
		}
		// TODO Auto-generated method stub
		return name;
	}

	@Override
	public List<String> requiredFields() {
		log.info("HllAggregatorFactory fieldName");
		// TODO Auto-generated method stub
		return Arrays.asList(fieldName);
	}

	@Override
	public byte[] getCacheKey() {
		// TODO Auto-generated method stub
		// return null;

		byte[] fieldNameBytes = fieldName.getBytes();
		return ByteBuffer.allocate(1 + fieldNameBytes.length).put((byte) 0x37)
				.put(fieldNameBytes).array();
	}

	@Override
	public String getTypeName() {
		// TODO Auto-generated method stub
		return "hll";
	}

	@Override
	public int getMaxIntermediateSize() {
		log.info("HllAggregatorFactory getMaxIntermediateSize");
		// TODO Auto-generated method stub
		return HllAggregator.m;
	}

	@Override
	public Object getAggregatorStartValue() {
		log.info("HllAggregatorFactory getAggregatorStartValue");
		// TODO Auto-generated method stub
		return new TIntByteHashMap();
	}

}

