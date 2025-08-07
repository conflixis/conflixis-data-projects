"""
Core Name Matching Module

This module provides name matching capabilities.
The latest implementation is available in:
/projects/005-core-name-matching-test/

Legacy implementations have been archived to:
/archive/name-matching-legacy/
"""

# For backward compatibility, we can import from archived versions if needed
import warnings

def legacy_import_warning():
    warnings.warn(
        "The name matching module has been updated. "
        "Please use the new implementation in /projects/005-core-name-matching-test/ "
        "Legacy code has been moved to /archive/name-matching-legacy/",
        DeprecationWarning,
        stacklevel=2
    )

# Export a placeholder for backward compatibility
__all__ = ['legacy_import_warning']
__version__ = '2.0.0'