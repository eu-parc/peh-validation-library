from peh_validation_library.error_report.error_collector import ErrorCollector


def test_singleton_behavior():
    collector1 = ErrorCollector()
    collector2 = ErrorCollector()
    assert collector1 is collector2, "ErrorCollector is not a singleton."

def test_add_error():
    collector = ErrorCollector()
    collector.clear_errors()  # Ensure a clean state
    collector.add_error("Error 1")
    assert "Error 1" in collector.get_errors(), "Error was not added correctly."

def test_get_errors():
    collector = ErrorCollector()
    collector.clear_errors()  # Ensure a clean state
    collector.add_error("Error 1")
    collector.add_error("Error 2")
    errors = collector.get_errors()
    assert errors == ["Error 1", "Error 2"], "Errors were not retrieved correctly."

def test_clear_errors():
    collector = ErrorCollector()
    collector.add_error("Error 1")
    collector.clear_errors()
    assert collector.get_errors() == [], "Errors were not cleared correctly."