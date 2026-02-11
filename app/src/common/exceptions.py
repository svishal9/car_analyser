class NumberNotInRangeError(Exception):
    # TODO: This is an example exception class. Can be modified  as per requirements
    #  or add more custom exceptions as needed.
    """Exception raised for errors in the input number.

    Attributes:
        num -- input number which caused the error
        message -- explanation of the error
    """

    def __init__(self, num, message="Number is not in (90, 9000) range"):
        self.num = num
        self.message = message
        super().__init__(self.message)
