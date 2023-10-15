package ObserverPattern;

import java.util.ArrayList;
import java.util.List;

public class IphoneObservableImpl implements StocksObservable {
	
	public List<NotificationAlertObserver> observerList=new ArrayList<>();
	public int stockCount=0;

	@Override
	public void add(NotificationAlertObserver observer ) {
		// TODO Auto-generated method stub
		observerList.add(observer);
		
	}
	
	@Override
	public void notifySubscriber() {
		// TODO Auto-generated method stub
		for(NotificationAlertObserver observer : observerList)
		{
			observer.update();
		}
	}

	@Override
	public int getCount() {
		// TODO Auto-generated method stub
		return stockCount;
		
	}

	@Override
	public void setCount(int newStock) {
		// TODO Auto-generated method stub

		if(stockCount == 0)
		{
			notifySubscriber();
		}
		stockCount = stockCount+newStock;
	}

	@Override
	public void remove(NotificationAlertObserver observer) {
		// TODO Auto-generated method stub
		observerList.remove(observer);
		
	}
	

}
