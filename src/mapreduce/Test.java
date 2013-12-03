package mapreduce;

import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.TimeUnit;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Calendar cal = new GregorianCalendar();
			
			cal.setTimeInMillis(1385639077000l);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			System.out.println(cal.getTimeInMillis());
			
		
	}

}
