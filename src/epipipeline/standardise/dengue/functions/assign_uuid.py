
def assign_uuid(n: int):
    """_summary_

    Args:
        n (int): length of dataset/number of uuuids to be generated

    Returns:
        pd.Series: series of uuid4 of length n; named metadata.recordID
    """
    assert isinstance(n, int), "Invalid Input: Enter an integer"
    
    import pandas as pd
    import uuid

    return pd.Series([uuid.uuid4() for i in range(n)], name="metadata.recordID")




