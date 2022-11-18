from abc import ABC, abstractmethod
import pandas as pd

class Loader(ABC):

    @abstractmethod
    def load(self,
        data: pd.DataFrame
    ) -> bool:
        pass