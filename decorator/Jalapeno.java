package DecoratorDesign;

public class Jalapeno extends ToppingDecorator {

	BasePizza bpizza;
	public Jalapeno(BasePizza base)
	{
		this.bpizza = base;
	}
	@Override
	public int cost() {
		// TODO Auto-generated method stub
		return  bpizza.cost() + 20;
	}

}
