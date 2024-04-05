# standardise dtypes
import re
import numpy as np

def NumvarStd(x):
    """_summary_

    Args:
        x (_type_): string/object variable

    Returns:
        _type_: numbers
    """
    if re.search(r"[^\d]", str(x)):
        res=re.search(r"\d+", str(x))
        if res:
            return res.group(0)
        else:
            return np.nan
    else:
        return x