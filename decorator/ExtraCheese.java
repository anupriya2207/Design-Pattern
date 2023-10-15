package DecoratorDesign;

public class ExtraCheese extends ToppingDecorator {

	BasePizza bpizza;
	
	public ExtraCheese(BasePizza base)
	{
		this.bpizza= base;
	}
	
	@Override
	public int cost() {
		// TODO Auto-generated method stub
		return bpizza.cost() + 20;
	}

}
