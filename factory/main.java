package FactoryDesign;

public class main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ShapeFactory s = new ShapeFactory();
		Shape sh = s.getShape("Square");
		sh.draw();
	}

}
