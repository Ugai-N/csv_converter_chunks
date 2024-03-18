import os
from abc import ABC, abstractmethod


class BasicConverter(ABC):
    """Abstract base class for different converting tools"""

    def __init__(self, input_file, output_path):
        self.input = input_file
        self.output = self.create_dir(output_path)
        self.__reading_time = None
        self.__converting_time = None
        self.__execution_time = None

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def info(self):
        if self.execution_time is not None:
            print(f"{self.__class__.__name__} (execution time): {self.execution_time:.2f} seconds")
            if self.reading_time is not None and self.converting_time is not None:
                print(f"including:\n{self.__class__.__name__} (reading time): {self.reading_time:.2f} seconds")
                print(f"{self.__class__.__name__} (converting time): {self.converting_time:.2f} seconds")
        else:
            raise Exception("Seems that the converter has not been launched")


    def create_dir(self, output):
        os.makedirs(output, exist_ok=True)
        return output

    @abstractmethod
    def run(self, *kwargs):
        pass

    @property
    def reading_time(self):
        return self.__reading_time

    @reading_time.setter
    def reading_time(self, value):
        self.__reading_time = value

    @property
    def converting_time(self):
        return self.__converting_time

    @converting_time.setter
    def converting_time(self, value):
        self.__converting_time = value

    @property
    def execution_time(self):
        if self.__execution_time is None:
            if self.__converting_time is not None and self.__reading_time is not None:
                return self.__converting_time + self.__reading_time
        return self.__execution_time

    @execution_time.setter
    def execution_time(self, value):
        self.__execution_time = value
