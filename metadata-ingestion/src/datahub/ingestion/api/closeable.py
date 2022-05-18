from abc import abstractmethod


class Closeable:
    @abstractmethod
    def close(self) -> None:
        pass
