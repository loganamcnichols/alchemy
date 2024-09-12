import re
import sqlite3
from typing import List, Literal, Optional, Union
from . import alchemy_types
from . import nullable_category_dtype

import pandas as pd
import numpy as np

RECORDS_DTYPES = {
  "survey_id":     np.int32,
  "response_id":   np.int32,
  "question":      np.str_,
  "subquestion":   np.str_,
  "option":        np.str_,
  "answer":        np.str_,
  "question_type": np.int8,
}


GET_RECORDS = '''
  SELECT 
    a.survey_id,
    a.response_id,
    q1.shortname as question,
    q2.title     as subquestion,
    o.value      as option,
    o.option_order,
    a.answer,
    q1.question_type
  FROM answer as a 
  INNER JOIN question as q1 ON q1.id = a.question_id
  LEFT  JOIN question as q2 ON q2.id = a.sub_question_id
  LEFT  JOIN option   as o  ON o.id  = a.option_id
  {where} ORDER BY a.survey_id, a.response_id, question;''';

def replace_non_alphanumeric(input_string):
    ret = re.sub(r'[^a-zA-Z0-9]', '_', input_string)
    if ret.startswith('X'):
        ret = ret[1:]
    return ret

class Alchemy():
  def __init__(self, db_path: str):
    self._conn = sqlite3.connect(db_path)
  
  def get_table(self, records:     Optional[pd.DataFrame]=None, 
                      survey_ids:  Optional[Union[int, List[int]]]=None, 
                      column_mode: Optional[Literal["flat", "multi"]]="flat") -> pd.DataFrame:
    if type(records) == None:
      records = self.get_records(survey_ids)
      return self._pivot_table(records)
    return self._pivot_table(records, survey_ids, column_mode)

  def get_records(self, survey_ids: Optional[Union[int, List[int]]]=None) -> pd.DataFrame:
    where_clauses = []
    params = []
    if survey_ids:
      if type(survey_ids) == int:
        where_clauses.append("a.survey_id = ?")
        params.append(survey_ids)
      elif type(survey_ids) == list:
        if type(survey_ids[0]) == int:
          where_clauses.append(f"a.survey_id IN ({','.join(['?'] * len(survey_ids))})")
          params += survey_ids
        else:
          raise ValueError(f"get_records expects `survey_ids` to be an int or list of ints, got list of {type(survey_ids[0])}")
      else:
        raise ValueError(f"get_table expects `survey_ids` to be an int or a list of ints, got {type(survey_ids)}")
    query = self._build_query(GET_RECORDS, where_clauses)
    return pd.read_sql_query(query, self._conn, params=params, dtype=RECORDS_DTYPES)

  def query(self, query: str) -> pd.DataFrame:
      return pd.read_sql(query, con=self._conn)

  def _build_query(self, select_clause: str, where_clauses: Optional[Union[list[str], str]]=None):
      query = select_clause
      where_stmt = ' AND '.join(where_clauses)
      if where_stmt: where_stmt = f"WHERE {where_stmt}"
      return query.format(where=where_stmt)
  
  def _pivot_table(self, records: pd.DataFrame, 
                   survey_ids:        Optional[Union[int, List[int]]]=None,
                   column_mode:      Optional[Literal["flat", "multi"]]="flat") -> pd.DataFrame:
    tmp_records = records.copy()
    if survey_ids:
      if type(survey_ids) == int:
        tmp_records = tmp_records.loc[records["survey_id"] == survey_ids]
      elif type(survey_ids) == list:
        if type(survey_ids[0]) == int:
          tmp_records = tmp_records.loc[records["survey_id"].isin(survey_ids)]
        else:
          raise ValueError(f"survey_ids must be either an integer or a list of integers, got list of f{type(survey_ids[0])}")
      else:
        raise ValueError(f"survey_ids must be either an integer or a list of integers, got {type(survey_ids)}")
    if column_mode == "flat":
      return self._flatten_table(tmp_records)
    else: 
      tmp_records["tmp_option"] = ""
      single_select = tmp_records.loc[tmp_records["question_type"].isin(alchemy_types.SINGLE_SELECT_QUESTIONS)]
      single_select_cats = (single_select.copy().drop_duplicates(subset=["question", "option"])
                                              .pivot(columns="question", index="option_order", values="option"))
      single_select = single_select.pivot(columns=["question", "subquestion", "tmp_option"], values="option", index=["survey_id", "response_id"])
      for question in single_select_cats.columns:
        cat = nullable_category_dtype.NullableCategory(single_select_cats[question].dropna().unique(), ordered=True) 
        single_select[question,"",""] = single_select[question,"",""].astype(cat)
      
      single_select.columns.set_names("option", level=2, inplace=True)
      
      multi_select = tmp_records.loc[tmp_records["question_type"].isin(alchemy_types.MULTI_SELECT_QUESTIONS)]

      multi_select = multi_select.pivot(columns=["question", "subquestion", "option"], values="answer", index=["survey_id", "response_id"])
      multi_select.replace({'0': 0, '1': 1}, inplace=True)
      multi_select = multi_select.astype('boolean')

      value = tmp_records.loc[tmp_records["question_type"].isin(alchemy_types.SINGLE_VALUE_QUESTION) | tmp_records["question_type"].isin(alchemy_types.MULTI_VALUE_QUESTIONS)]
      value = value.pivot(columns=["question", "subquestion", "option"], values="answer", index=["survey_id", "response_id"])

      return pd.concat([single_select, multi_select, value], axis=1)
   
  def _flatten_table(self, records: pd.DataFrame) -> pd.DataFrame:
    records["variable_name"] = ""
    records.loc[records["question_type"].isin(alchemy_types.SINGLETON_QUESTIONS), "variable_name"] = (
    records.loc[records["question_type"].isin(alchemy_types.SINGLETON_QUESTIONS), "question"]
    )

    records.loc[records["question_type"].isin(alchemy_types.MULTI_QUESTIONS), "variable_name"] = (
    records.loc[records["question_type"].isin(alchemy_types.MULTI_QUESTIONS), "option"]
           .str.replace('[^a-zA-Z0-9]', '_', regex=True)
           .str.lstrip('X') + '_' +
    records.loc[records["question_type"].isin(alchemy_types.MULTI_QUESTIONS), "question"]
           .str.replace('[^a-zA-Z0-9]', '_', regex=True)
    )

    records.loc[records["question_type"].isin(alchemy_types.TWO_LAYER_QUESTIONS), "variable_name"] = (
    records.loc[records["question_type"].isin(alchemy_types.TWO_LAYER_QUESTIONS), "subquestion"]
           .str.replace('[^a-zA-Z0-9]', '_', regex=True)
           .str.lstrip('X') + '_' +
    records.loc[records["question_type"].isin(alchemy_types.TWO_LAYER_QUESTIONS), "question"]
           .str.replace('[^a-zA-Z0-9]', '_', regex=True)
    )

    records.loc[records["variable_name"].str.contains(r'^\d', regex=True), "variable_name"] = (
    records.loc[records["variable_name"].str.contains(r'^\d', regex=True), "variable_name"]
            .str.pad(1, 'left', 'X') 
    )
    records.drop_duplicates(subset=['survey_id', 'response_id', 'variable_name'], inplace=True)


    return records.pivot(index=["survey_id", "response_id"], columns="variable_name", values="answer")


    


    
# alchemy = Alchemy("alchemy.db")
# records = alchemy.get_records(survey_ids=[7982666, 8002909])
# table = alchemy.get_table(records=records, survey_ids=8002909)
