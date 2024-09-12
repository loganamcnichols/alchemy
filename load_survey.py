import argparse
import os
from typing import Any, Optional, Tuple
import json
from dotenv import load_dotenv
import requests
import sqlite3
import logging
from alchemy.alchemy_types import *


SURVEY_STATIC_CHECK = """
UPDATE survey
  SET title  = :title
  WHERE id   = :id    AND
      title != :title;
"""

SURVEY_INSERT_STMT = """
INSERT OR IGNORE INTO 
       survey (id, title) 
       VALUES (:id, :title);
"""

RESPONSE_INSERT_STMT = """
INSERT OR IGNORE INTO
          response(id, survey_id)
          VALUES(:id, :survey_id);
"""



ANSWER_INSERT_STMT = """
INSERT INTO
  answer(question_id,  sub_question_id,  option_id,  response_id,  survey_id,  answer)
  VALUES(:question_id, :sub_question_id, :option_id, :response_id, :survey_id, :answer);
"""

# Test query, should alter 0 rows
QUESTION_STATIC_CHECK = """
  UPDATE question
     SET shortname  = :shortname
   WHERE id         = :id
     AND shortname != :shortname
"""

QUESTION_INSERT_STMT = """
INSERT OR IGNORE INTO 
  question (id,  title,  base_type,  question_type, shortname)
  VALUES   (:id, :title, :base_type, :type,         :shortname);
"""

SURVEY_X_QUESTION_INSERT_STMT = """
INSERT OR IGNORE INTO
  survey_x_question (question_id, survey_id)
             VALUES (:id,         :survey_id); 
"""

OPTION_STATIC_CHECK = """
UPDATE  option
   SET  value =  :value
 WHERE  id    =  :id
   AND  value != :value;
"""

OPTIONS_INSERT_STMT = """
INSERT OR IGNORE INTO
  option (id,  value, option_order)
  VALUES (:id, :value, :option_order);
"""


def iter_over(json_data, key: str = None):
  if not key:
    return _iter_over(json_data)
  if key not in json_data: 
    return []
  return _iter_over(json_data[key])

def _iter_over(json_data):
  return (json_data 
          if   type(json_data) == list
          else json_data.values()
          if   type(json_data) == dict
          else [])
  
          

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create handlers
c_handler = logging.StreamHandler()  # Console handler
f_handler = logging.FileHandler('file.log')  # File handler
c_handler.setLevel(logging.INFO)
f_handler.setLevel(logging.DEBUG)

# Create formatters and add it to handlers
log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(log_format)
f_handler.setFormatter(log_format)

# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)

def execute(cursor: sqlite3.Cursor, query: str, params: Any) -> Tuple[int, Optional[str]]:
  try:
    cursor.execute(query, params)
    return cursor.rowcount, None
  except Exception as e:
    return cursor.rowcount, f"error executing query {query} with params {params}: {str(e)}"
 
def executemany(cursor: sqlite3.Cursor, query: str, params: Any, suppress_output=False) -> Tuple[int, Optional[str]]:
  row_count = 0
  for param in params:
    try:
      cursor.execute(query, param)
      row_count += cursor.rowcount
    except Exception as e:
      if not suppress_output:
        logger.warning(f"error executing query {query} with params {param}: {str(e)}")
  return row_count, None

def load_survey(con: sqlite3.Connection, survey_id: str, 
                api_key: str, api_secret: str) -> Optional[str]:

  logger.info(f"processing data for survey {survey_id}")
  cursor = con.cursor()
  
  if not api_key:
    return 0, f"no api key provided"
  if not api_secret:
    return 0, f"no api secret provided"

  alchemer_host = "https://api.alchemer.com"
  survey_path   = f"{alchemer_host}/v5/survey/{survey_id}"
  question_path = f"{survey_path}/surveyquestion"
  response_path = f"{survey_path}/surveyresponse"
   
  res = requests.get(survey_path, params={'api_token': api_key, 'api_token_secret': api_secret})
  if res.status_code != 200:
    return 0, f"recieved {res.status_code} response when fetching {survey_id}: {res.reason}"
    
  survey_data = res.json()["data"]
  assert(survey_data["id"] == survey_id)
  
  rowcount, err = execute(cursor, SURVEY_STATIC_CHECK, survey_data)
  if err != None:
    return err
  elif rowcount != 0:
    logger.warn(f"Survey static check failed: {SURVEY_STATIC_CHECK} updated {rowcount} rows")

  rowcount, err = execute(cursor, SURVEY_INSERT_STMT, survey_data)
  if err != None:
    return err

  logger.info(f"inserted {cursor.rowcount} new survey(s) into the database")
  
  res = requests.get(question_path, params={'api_token': api_key, 'api_token_secret': api_secret})
  if res.status_code != 200:
    return f"received {res.status_code} response when fetching {survey_id}: {res.reason}" 
  
  res_json = res.json()
  questions_data = res_json["data"]
  options_data   = []

  if (res_json["page"] != res_json["total_pages"]):
    logger.warn(f"reporting multiple pages of question data. Data may be incomplete")
  if len(questions_data) != res_json["results_per_page"]:
    logger.warn(f"reporting different 'results_per_page' then our data length: {len(questions_data)} vs {res_json['results_per_page']}")

  i = 0
  while i < len(questions_data):
    question_data = questions_data[i]
    if (question_data["type"] not in QUESTION_TYPE_STR_MAP or
        question_data["base_type"] not in BASE_TYPE_STR_MAP):
      return f"""base type / question type {question_data['base_type']} / {question_data['type']} 
                found in survey is missing from our lookup table. Exiting early."""
    question_data["type"]      = QUESTION_TYPE_STR_MAP[question_data["type"]].value
    question_data["base_type"] = BASE_TYPE_STR_MAP[question_data["base_type"]].value
    question_data["title"]     = question_data["title"]["English"]
    question_data["shortname"] = question_data["shortname"] or question_data["title"]
    question_data["survey_id"] = survey_id;
    for sub_answer in question_data.get("sub_questions", []):
      questions_data.append(sub_answer)
    for j, question_option in enumerate(question_data.get("options", [])):
      question_option["option_order"] = j 
      options_data.append(question_option)
    i += 1

  logger.info(f"starting to process questions for survey {survey_id}")
  
  rowcount, err = executemany(cursor, QUESTION_STATIC_CHECK, questions_data)
  if err != None:
    return err
  elif rowcount != 0:
    logger.warn(f"static check failed: {QUESTION_STATIC_CHECK} modified {rowcount} rows")

  rowcount, err = executemany(cursor, QUESTION_INSERT_STMT, questions_data)
  if err != None:
    return err
  
  logger.info(f"inserted {rowcount} new questions into the database")
  
  _, err = executemany(cursor, SURVEY_X_QUESTION_INSERT_STMT, questions_data)
  if err != None:
    return err
  
  rowcount, err = executemany(cursor, OPTION_STATIC_CHECK, options_data)
  if err != None:
    return err
  elif rowcount != 0:
    logger.warn(f"static check failed: {OPTION_STATIC_CHECK} modified {rowcount} rows")
    
  rowcount, err = executemany(cursor, OPTIONS_INSERT_STMT, options_data)
  if err != None:
    return f"error executing command '{OPTIONS_INSERT_STMT}': {err}"

  logger.info(f"added {rowcount} options to the database")
  
  res = requests.get(response_path, params={'api_token': api_key, 
                                            'api_token_secret': api_secret, 
                                            'resultsperpage': 100})
  if res.status_code != 200:
    return f"received {res.status_code} response when fetching {survey_id}: {res.reason}"

  res_data       = res.json()
  total_pages    = res_data["total_pages"]
  response_count = res_data["total_count"] 
  page = 1

  logger.info(f"{total_pages} pages to process")
  logger.info(f"{response_count} responses")


  while page < res_data['total_pages']:
    logger.info(f"processing page {page}")
    res = requests.get(response_path, params={'api_token': api_key, 
                                              'api_token_secret': api_secret, 
                                              'resultsperpage': 100,
                                              'page': page})
    if res.status_code != 200:
      return f"received {res.status_code} response when fetching {survey_id}: {res.reason}"

    responses = res.json()["data"]

    for response in responses:
      response["survey_id"] = survey_id

    total_answers = 0
    null_answers  = 0
    for response in responses:
      total_answers += len(iter_over(response, "survey_data"))
    
    logger.info(f"total potential answers for page are {total_answers}")
      
    
    _, err = executemany(cursor, RESPONSE_INSERT_STMT, responses)
    if err != None:
      err
    parsed_answers = []
    for response in responses:
      parsed_answer = {"response_id": response["id"], "survey_id": survey_id}
      answers = response["survey_data"]
      for answer in iter_over(answers):
        cursor.execute("SELECT * FROM question WHERE id = ?", (answer["id"],))
        question = cursor.fetchone()
        if not question:
          return f"No question matching response id {response['id']}. This shouldn't be possible"
        qtype = question["question_type"]
        if answer.get("parent"):
          parsed_answer["question_id"] = answer["parent"]
          parsed_answer["sub_question_id"] = answer["id"]
        else:
          parsed_answer["question_id"] = answer["id"]
          parsed_answer["sub_question_id"] = 0
        if qtype == QuestionType.HIDDEN:
          parsed_answer["option_id"]       = 0
          parsed_answer["answer"]          = answer.get("answer", 0)
          parsed_answers.append(parsed_answer.copy())
        elif qtype in SINGLE_SELECT_QUESTIONS:
          if not answer.get("answer_id"): 
            null_answers += 1
            continue # TODO: should we handle this differently?
          parsed_answer["option_id"]       = answer["answer_id"]
          parsed_answer["answer"]          = "answer" in answer
          parsed_answers.append(parsed_answer.copy())
        elif qtype in SINGLE_VALUE_QUESTION:
          parsed_answer["option_id"]       = 0
          parsed_answer["answer"]          = answer.get("answer")
          parsed_answers.append(parsed_answer.copy())
        elif qtype in MULTI_SELECT_QUESTIONS:
          for option in iter_over(answer, "options"):
            parsed_answer["option_id"]     = option["id"] 
            parsed_answer["answer"]        = "answer" in option 
            parsed_answers.append(parsed_answer.copy())
        elif qtype in MULTI_VALUE_QUESTIONS:
          for option in iter_over(answer, "options"):
            parsed_answer["option_id"]     = option["id"] 
            parsed_answer["answer"]        = option.get("answer") 
            parsed_answers.append(parsed_answer.copy())
        elif qtype in TWO_LAYER_QUESTIONS:
          for sub_answer in iter_over(answer, "sub_questions"):
            parsed_answer["sub_question_id"] = sub_answer["id"]
            cursor.execute("SELECT * FROM question WHERE id = ?", sub_answer["id"])
            row = cursor.fetchone()
            if not row:
              return f"No question matching response id {response['id']}. This shouldn't be possible"
            sub_question_qtype = row["question_type"]
            if sub_question_qtype in SINGLE_SELECT_QUESTIONS:
              parsed_answer["option_id"]       = sub_answer["answer_id"]
              parsed_answer["answer"]          = "answer" in sub_answer 
              parsed_answers.append(parsed_answer.copy())
            elif sub_question_qtype in SINGLE_VALUE_QUESTION:
              parsed_answer["option_id"]       = 0
              parsed_answer["answer"]          = sub_answer.get("answer")
              parsed_answers.append(parsed_answer.copy())
            elif sub_question_qtype in MULTI_SELECT_QUESTIONS:
              for option in iter_over(sub_answer, "options"):
                parsed_answer["option_id"]     = option["id"] 
                parsed_answer["answer"]        = "answer" in option 
                parsed_answers.append(parsed_answer.copy())
            elif sub_question_qtype in MULTI_VALUE_QUESTIONS:
              for option in iter_over(sub_answer, "options"):
                parsed_answer["option_id"]     = option["id"] 
                parsed_answer["answer"]        = option.get("answer") 
                parsed_answers.append(parsed_answer.copy())
            else:
              return f"What the hell am I supposed to do with this answer? {qtype} {answer}"
        else:
          return f"Encountered question type we don't know how to handle yet: type: {qtype}, answer: {answer}"
    
    logger.info(f"total potential answers: {total_answers}. null answers {null_answers}. Remaining {total_answers - null_answers}. Successfully parses {len(parsed_answers)}")

    rowcount, err = executemany(cursor, ANSWER_INSERT_STMT, parsed_answers, True)
    if err != None:
      return err
    
    page += 1

    logger.info(f"added {rowcount} answers to the database")

  return None
  
  
  
def check_type_coverage(con: sqlite3.Connection, api_key: str, api_secret: str):
  alchemer_host = "https://api.alchemer.com"
  surveys_path  = f"{alchemer_host}/v5/survey"
  survey_question_list = [
    {"survey_id": 8002909, "question_id": 249},
    {"survey_id": 8002909, "question_id": 142},
    {"survey_id": 8002909, "question_id": 3},
    {"survey_id": 8002909, "question_id": 4},
    {"survey_id": 8002909, "question_id": 264},
    {"survey_id": 8002909, "question_id": 1},
    {"survey_id": 8000887, "question_id": 237},
    {"survey_id": 8000887, "question_id": 17},
    {"survey_id": 8000887, "question_id": 20},
    {"survey_id": 8000887, "question_id": 150},
    {"survey_id": 7999990, "question_id": 4038},
    {"survey_id": 7999990, "question_id": 672},
    {"survey_id": 7984794, "question_id": 4251},
    {"survey_id": 7927840, "question_id": 3456},
    {"survey_id": 7896178, "question_id": 136},
    {"survey_id": 7825431, "question_id": 2939},
    {"survey_id": 7686970, "question_id": 2134},
    {"survey_id": 7565583, "question_id": 449},
    {"survey_id": 7442766, "question_id": 171}
  ]
  for survey_question in survey_question_list:
    survey_id   = survey_question["survey_id"]
    question_id = survey_question["question_id"]
    survey_question_path = f"{surveys_path}/{survey_id}/surveyquestion/{question_id}"
    res = requests.get(survey_question_path, params={'api_token': api_key, 'api_token_secret': api_secret, 'resultsperpage': 300})
    if res.status_code != 200:
      return 0, f"recieved {res.status_code} response when fetching {survey_id}: {res.reason}"
    
    question_data = res.json()["data"]
    logger.info(question_data)

if __name__ == "__main__":
    con = sqlite3.connect("alchemy.db")
    con.row_factory = sqlite3.Row
    cursor = con.cursor()
    cursor.execute('PRAGMA foreign_keys = ON;')

    parser = argparse.ArgumentParser(description="A script that accepts a variable number of arguments.")
    parser.add_argument("survey_ids", nargs="*", type=str, help="survey ids to fetch")
    args = parser.parse_args()
    
    if len(args.survey_ids) == 0:
      print("Valid usage: python3 load_survey.py <survey_id>")
      exit

    load_dotenv()
    api_key    = os.environ.get("API_KEY")
    api_secret = os.environ.get("API_SECRET")


    for survey_id in args.survey_ids:
      con.execute("BEGIN TRANSACTION;")
      err = load_survey(con, survey_id, api_key, api_secret)
      if err == None:
        con.commit()
      else: 
        logging.error(err)
        con.rollback()
        logging.info(f"all data just added for suvey_id {survey_id} has been rolled back")