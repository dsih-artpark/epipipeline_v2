import re
import numpy as np

def standardise_result(x) -> str:
    """This function standardises results to positive or negative

    Args:
        x (str/int): Result in the raw dataset

    Returns:
        str: Negative, Positive or NaN
    """
    if isinstance(x, str) or isinstance(x, int):
        if re.search(r"-ve|Neg|Negative|No|0", str(x), re.IGNORECASE):
            return "NEGATIVE"
        elif re.search(r"NS1|IgM|D|Yes|\+ve|Pos|Positive|1", str(x), re.IGNORECASE):
            return "POSITIVE"
    return np.nan

