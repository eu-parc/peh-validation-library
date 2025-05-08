class ErrorReport:
    def __init__(self):
        self.errors = []

    def add_error(self, error: Exception) -> None:
        """
        Add an error to the report.

        Args:
            error (Exception): The error to add.
        """
        self.errors.append(error)

    def get_errors(self) -> list[Exception]:
        """
        Get the list of errors.

        Returns:
            list[Exception]: The list of errors.
        """
        return self.errors
