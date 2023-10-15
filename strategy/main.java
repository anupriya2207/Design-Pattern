package strategy;

public class main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Hello World");
		Vehicle n = new Vehicle(new NormalVehicleStrategy());
		Vehicle s = new Vehicle(new SportVehicleStrategy());
		s.drive();
		n.drive();
	}

}
