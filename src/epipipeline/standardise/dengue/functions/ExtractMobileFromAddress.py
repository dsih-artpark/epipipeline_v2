import pandas as pd
import re
import numpy as np

def extract_mobile_from_address(address: str) -> tuple:
    """This function extracts mobile number from the address/name fields and strips the name/address from the mobile number field

    Args:
        address (str): Name & Address in KA, and Address in PMC, PCMC, pune Rural

    Returns:
        tuple: DataFrame series of address & mobile number
    """
    assert isinstance(address, str), "Invalid input"

    mobile_present=re.search(r"(9?1?\d{10})", address)

    if (mobile_present):
        mobile_number=mobile_present.group(1)
        address=re.sub(r"9?1?\d{10}","", address)
    else:
        mobile_number=np.nan
    
    return (address, mobile_number)
