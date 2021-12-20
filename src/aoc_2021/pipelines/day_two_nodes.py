from kedro.pipeline import node
import pandas as pd


nodes = []

def clean_day2_data(data: str) -> pd.DataFrame:
    for rows in data:
        rows = [x.strip().split() for x in rows if x.strip()]    
    return pd.DataFrame(rows)
    

def find_position(day_two: pd.DataFrame) -> str:
    """count_increases.

    Args:
        day_one (pd.DataFrame): day_one sonar measurements

    Returns:
        str: count of increases as STRING becuase STUPID
    """
    diffs = day_two.diff()
    res: pd.Series = diffs[diffs > 0].count()
    return str(res.values[0])


nodes.append(
    node(
        func=clean_day2_data,
        inputs="a_raw_day_two_test",
        # inputs="a_raw_day_two",
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
        outputs="d_fea_day_two_count",
        name="create_d_fea_day_two_count",
    )
)  



