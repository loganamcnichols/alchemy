PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;

CREATE TABLE survey (
  id          INTEGER PRIMARY KEY,
  title       TEXT    NOT NULL UNIQUE
);

CREATE TABLE question (
  id            INTEGER PRIMARY KEY,
  base_type     INTEGER NOT NULL,
  question_type INTEGER NOT NULL,
  title         TEXT    NOT NULL,
  shortname     TEXT    NOT NULL
);

CREATE TABLE option (
  id            INTEGER PRIMARY KEY,
  value         TEXT,
  option_order  INTEGER
);


CREATE TABLE survey_x_question (
  survey_id     INTEGER,
  question_id   INTEGER,
  UNIQUE(survey_id, question_id),
  FOREIGN KEY (survey_id) REFERENCES survey(id) ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (question_id) REFERENCES question(id) ON UPDATE RESTRICT ON DELETE RESTRICT
  );

CREATE TABLE response (
  id             INTEGER,
  survey_id      INTEGER,
  UNIQUE (id, survey_id),
  FOREIGN KEY (survey_id) REFERENCES survey(id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE answer (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  question_id     INTEGER,
  sub_question_id INTEGER,
  option_id       INTEGER,
  response_id     INTEGER,
  survey_id       INTEGER,
  answer          TEXT,
  FOREIGN KEY (sub_question_id) REFERENCES question(id) ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (question_id) REFERENCES question(id) ON UPDATE RESTRICT ON DELETE RESTRICT,
  FOREIGN KEY (response_id, survey_id) REFERENCES response(id, survey_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

INSERT INTO question (id, base_type, question_type, title, shortname)
              VALUES (0, 0, 0, "", "");
INSERT INTO   option (id, value)
              VALUES (0, "");

COMMIT;