CREATE TABLE IF NOT EXISTS instruments (
  id INTEGER PRIMARY KEY, 
  isin TEXT UNIQUE,
  description TEXT,
  timestamp TEXT
);

CREATE TABLE IF NOT EXISTS quotes (
  id INTEGER PRIMARY KEY, 
  isin TEXT UNIQUE,
  price REAL,
  timestamp TEXT
);

CREATE INDEX idx_quotes_isin
ON quotes(isin);