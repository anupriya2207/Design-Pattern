package strategy;

public class Vehicle{
	
	VehicleStrategy obj;
	Vehicle(VehicleStrategy ob)// dependency injection or constructor injection
	{
		this.obj=ob;
	}

		public  void drive()
		{
			obj.drive();
		}
}