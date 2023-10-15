package ObserverPattern;

public class Store {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		 StocksObservable iphone = new IphoneObservableImpl();
		 
		 NotificationAlertObserver observer1 =new EmailAlertObserverImpl("anu@123",iphone);
		 iphone.add(observer1);
		 iphone.setCount(10);
		 
	}

}
