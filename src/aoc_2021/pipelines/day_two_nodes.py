from typing import List
from kedro.pipeline import node
import pandas as pd


nodes = []

def clean_day2_data(data: str) -> pd.DataFrame:
    new_data = [x for x in data.split("\n") if x]
    _df = pd.DataFrame(new_data)
    _df[[0,1]] = _df[0].str.split(' ',expand=True)
    return _df
    

def find_position(day_two: pd.DataFrame) -> str:
    """find_position.

    Args:
        day_two (pd.DataFrame): day_two movements

    Returns:
        str: final position as STRING
    """
    forward_position = get_sum_of_distance(filter_dataframe_by_direction(day_two, "forward"))
    up_position = get_sum_of_distance(filter_dataframe_by_direction(day_two, "up"))
    down_position = get_sum_of_distance(filter_dataframe_by_direction(day_two, "down"))
    return str(calculate_final_position(forward_position, up_position, down_position))
    
    
def calculate_final_position(forward: int, up: int, down: int) -> int:
    """find_position.

    Args:
        forward (int): forward distance traveled
        up (int): up distance traveled
        down (int): down distance traveled

    Returns:
        int: number representig final position
    """
    return abs((up - down)) * forward
    


def filter_dataframe_by_direction(day_two: pd.DataFrame, direction: str) -> pd.DataFrame:
    """filter_dataframe_by_direction.

    Args:
        day_two (pd.DataFrame): day_two movements
        direction (Str): direction to be filtered by

    Returns:
        pd.DataFrame: filterd by direction
    """
    return day_two[day_two[0].eq(direction)]


def get_sum_of_distance(df: pd.DataFrame) -> int:
    """get_sum_of_distance.

    Args:
        df (pd.DataFrame): movements filtered by direction

    Returns:
        int: total distance traveled in particular direction
    """
    return df[1].astype(int).sum()


nodes.append(
    node(
        func=clean_day2_data,
        #inputs="a_raw_day_two_test",
        inputs="a_raw_day_two",
        outputs="b_int_day_two",
        name="create_b_int_day_two",
    )
)
nodes.append(
    node(
        func=lambda df: df,
        inputs="b_int_day_two",
        outputs="c_pri_day_two",
        name="create_c_pri_day_two",
    )
)

nodes.append(
    node(
        func=find_position,
        inputs=["c_pri_day_two"],
        outputs="d_fea_day_two_position",
        name="create_d_fea_day_two_position",
    )
)  



