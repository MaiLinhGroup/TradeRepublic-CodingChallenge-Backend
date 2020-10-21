CREATE TABLE IF NOT EXISTS instruments (
  id INTEGER PRIMARY KEY, 
  isin TEXT UNIQUE,
  description TEXT,
  timestamp TEXT
);

CREATE TABLE IF NOT EXISTS quotes (
  id INTEGER PRIMARY KEY, 
  isin TEXT,
  price REAL,
  timestamp TEXT
);

CREATE INDEX IF NOT EXISTS idx_quotes_isin
ON quotes(isin);
