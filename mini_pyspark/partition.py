class Partition:
    """
    Represents a chunk of the dataset.
    """
    def __init__(self, index, data_slice):
        self.index = index
        self.data = data_slice