def safe_round(number, ndigits):
    try:
        return round(number, ndigits)
    except Exception as e:
        raise Exception(f"Error rounding number {number}: {e}")