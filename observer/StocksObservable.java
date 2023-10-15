package ObserverPattern;

public interface StocksObservable {
	
	public void add(NotificationAlertObserver observer);
	public void remove(NotificationAlertObserver observer);
	public void notifySubscriber();
	public int getCount();
	public void setCount(int newStockAdded);

}
