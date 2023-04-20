"""Some utlity functions."""
from pathlib import Path


def get_project_root() -> Path:
    """Get the root directory of the project.

    Returns:
        Path: The root directory
    """
    return Path(__file__).parent.parent


if __name__ == "__main__":
    print(get_project_root() / "data")
