import os
import sys
topdir = os.path.join(os.path.dirname(__file__), "..")
sys.path.append(topdir)

from src import app

# from main import run_app
def test_function():
    """Test function."""
    print("HERE")
    value = "test"
    assert value == "test"

def test_function2():
    """Test function."""
    value = "test"
    assert value == "test"


# def test_function3():
#     """Test function."""
#     print(app)
#     value = "test"
#     assert value == "test"