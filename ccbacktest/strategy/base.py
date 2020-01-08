import abc


class Strategy(abc.ABC):

  @abc.abstractmethod
  @property
  def backend():
    pass
  
  @abc.abstractmethod
  @property
  def data_loaders():
    pass
  
  @abc.abstractmethod
  def initialize(self):
    pass
  
  @abc.abstractmethod
  def schedule():
    pass

  @abc.abstractmethod
  def get_portfolio():
    pass

  @abc.abstractmethod
  def order():
    pass
  
  @abc.abstractmethod
  def record():
    pass
