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
		for(int i=0; i< 1000;i++){
			
			cal.setTimeInMillis(new Date().getTime());
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			System.out.println(cal.getTimeInMillis());
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
