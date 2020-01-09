import abc

class BasePortfolio(abc.ABC):
    def __init__(self, backend, intial_balance: dict = None):
      self.base_currency = "BTC"
      self.value = None
      self._backend = backend
      self._balance = initial_balance

    @abc.abstractmethod
    def _evaluate(sef, t: int):
        pass
    
    @property
    def base(self):
        return self.base_currency
    
    @abc.abstractmethod
    def current_time(self):
        pass
    
    @base.setter
    def base(self, value):
        self.base_currency = value
        self.value = self._evaluate(self._current_time)
    
    @property
    def backend(self):
        return self._backend
    
    @backend.setter
    def backend(self, value):
        raise Exception("Backend can only be set once")
     
   @abc.abstractmethod
    def deduce_fee(self, value, base, t=None):
        """
        t: timestep at which deduce fee (used in simulation mode)
        """
        pass
    
    @abc.abstractmethod
    def buy(self, value, base, t=None, order_type="market", limit=None):
        pass
    
    @abc.abstractmethod
    def sell(self, value, base, t=None, order_type="market", limit=None):
        pass
    
    @abc.abstractmethod
    def sell_percentage(self, percentage, base, t=None, order_type="market", limit=None):
        pass
    
    @abc.abstractmethod
    def buy_portfolio_percentage(self, percentage, base, t=None, order_type="market", limit=None):
        pass

    
    
    
    
    
        
    
