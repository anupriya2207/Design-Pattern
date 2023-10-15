package ObserverPattern;

public class EmailAlertObserverImpl implements NotificationAlertObserver {
	
	String username;
	StocksObservable observer;
	
	public EmailAlertObserverImpl(String emailId, StocksObservable observer )
	{
		this.username=emailId;
		this.observer=observer;
	}

	@Override
	public void update() {
		// TODO Auto-generated method stub
		sendEmail(username, "product is available");
	}
	public void sendEmail(String uname, String msg)
	{
		System.out.println("msg sent to :"+ username);
	}

}
