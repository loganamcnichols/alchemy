from enum import Enum, IntEnum

INT    = 0
FLOAT  = 1
STRING = 2

class ResponseStatus(IntEnum):
  DISQUALIFIED = 0
  PARTIAL      = 1
  COMPLETE     = 2

class SurveyType(IntEnum):
  SURVEY = 0
  POLL   = 1
  FORM   = 2
  QUIZ   = 3
  
class SurveyStatus(IntEnum):
  LAUNCHED = 0
  CLOSED   = 1


class BaseType(IntEnum):
  ACTION = 1
  QUESTION = 2
  DECORATIVE = 3


class QuestionType(IntEnum):
  HIDDEN        = 0 
  INSTRUCTIONS  = 1
  JAVASCRIPT    = 2
  URLREDIRECT   = 3
  LOGIC         = 4
  MEDIA         = 5
  RADIO         = 6  
  MENU          = 7  
  SLIDER        = 8 
  TEXTBOX       = 9
  ESSAY         = 10
  CHECKBOX      = 11 
  RANK          = 12
  MULTI_TEXTBOX = 13
  MULTI_SLIDER  = 14
  GROUP         = 15
  VIDEO         = 16
  TABLE         = 17
  MATRIX        = 18

SINGLE_SELECT_QUESTIONS = [QuestionType.RADIO, QuestionType.MENU]
SINGLE_VALUE_QUESTION   = [QuestionType.SLIDER, QuestionType.TEXTBOX, QuestionType.ESSAY]

SINGLETON_QUESTIONS     = SINGLE_SELECT_QUESTIONS + SINGLE_VALUE_QUESTION + [QuestionType.HIDDEN]

MULTI_SELECT_QUESTIONS = [QuestionType.CHECKBOX]
MULTI_VALUE_QUESTIONS  = [QuestionType.RANK, QuestionType.MULTI_SLIDER, QuestionType.MULTI_TEXTBOX, QuestionType.VIDEO]
MULTI_QUESTIONS        = [MULTI_SELECT_QUESTIONS] + [MULTI_VALUE_QUESTIONS]

TWO_LAYER_QUESTIONS = [QuestionType.TABLE, QuestionType.MATRIX]

NOT_IMPLEMENTED = [QuestionType.INSTRUCTIONS, QuestionType.JAVASCRIPT, QuestionType.URLREDIRECT, QuestionType.LOGIC]




QUESTION_TYPE_STR_MAP = {
'HIDDEN':             QuestionType.HIDDEN,        
# Singleton questions 
'MENU':               QuestionType.MENU,          
'TEXTBOX':            QuestionType.TEXTBOX,       
'RADIO':              QuestionType.RADIO,         
'ESSAY':              QuestionType.ESSAY,         
'JAVASCRIPT':         QuestionType.JAVASCRIPT,    
'CHECKBOX':           QuestionType.CHECKBOX,      
'TABLE':              QuestionType.TABLE,         
'URLREDIRECT':        QuestionType.URLREDIRECT,   
'INSTRUCTIONS':       QuestionType.INSTRUCTIONS,  
'LOGIC':              QuestionType.LOGIC,         
'MULTI_TEXTBOX':      QuestionType.MULTI_TEXTBOX, 
'VIDEO':              QuestionType.VIDEO,         
'GROUP':              QuestionType.GROUP,         
'MEDIA':              QuestionType.MEDIA,         
'SLIDER':             QuestionType.SLIDER,        
'MULTI_SLIDER':       QuestionType.MULTI_SLIDER,  
'RANK':               QuestionType.RANK,          
'MATRIX':             QuestionType.MATRIX         
}                                                 


class BaseType(IntEnum):
  ACTION     = 1
  QUESTION   = 2
  DECORATIVE = 3

BASE_TYPE_STR_MAP = {
'Action':         BaseType.ACTION,     
'Question':       BaseType.QUESTION,   
'Decorative':     BaseType.DECORATIVE, 
}                                      
  
  

