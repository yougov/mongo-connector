"""Defines a checkpoint class for timestamps
"""

class Checkpoint():
    """Represents a checkpoint object that has a commit_ts field (for now).
    """
    
    def __init__(self):
        """Initialize the checkpoint to none.
        """
        self.commit_ts = None
        