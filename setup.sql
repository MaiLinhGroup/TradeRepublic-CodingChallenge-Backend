CREATE TABLE IF NOT EXISTS instruments (
  -- it increments by default without need for AUTOINCREMENT
  -- https://www.sqlite.org/autoinc.html
  id INTEGER PRIMARY KEY, 
  isin TEXT UNIQUE,
  description TEXT,
  timestamp TEXT
);

CREATE TABLE IF NOT EXISTS quotes (
  -- it increments by default without need for AUTOINCREMENT
  -- https://www.sqlite.org/autoinc.html
  id INTEGER PRIMARY KEY, 
  isin TEXT UNIQUE,
  price REAL,
  timestamp TEXT
);

CREATE INDEX idx_quotes_isin
ON quotes(isin);
