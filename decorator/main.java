package DecoratorDesign;

public class main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		BasePizza base = new Jalapeno(new ExtraCheese(new Margritha()));
		int a=base.cost();
		System.out.println(a);
	}

}
