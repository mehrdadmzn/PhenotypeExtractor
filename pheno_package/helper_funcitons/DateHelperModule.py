# %%
import datetime


# %%
def str_to_date(date_str: str) -> datetime.datetime.date:
    """

    Args:
        date_str: Date string in YYYY-mm-dd format

    Returns:
        date_date: the datetime.datetime.date equivalent of date_str

    """
    # todo: test the format of the input date_str
    date_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
