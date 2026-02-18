-- Run this in your Supabase SQL editor
-- Table for daily stock prices from the BRVM bulletin

CREATE TABLE IF NOT EXISTS brvm_cotation_journaliere (
  id                BIGSERIAL PRIMARY KEY,
  date              DATE        NOT NULL,
  ticker            TEXT        NOT NULL,
  compagnie         TEXT,
  secteur           TEXT,
  cours_precedent   NUMERIC,
  cours_ouv         NUMERIC,
  cours_cloture     NUMERIC,
  variation_jour    NUMERIC,      -- % ex: 7.49 means +7.49%
  volume            BIGINT,
  valeur_transigee  BIGINT,       -- FCFA
  cours_reference   NUMERIC,
  variation_ytd     NUMERIC,      -- % year-to-date
  dernier_div       NUMERIC,      -- last dividend per share (FCFA)
  date_div          TEXT,         -- "29-juil.-25" (raw from PDF)
  rendement_net     NUMERIC,      -- dividend yield %
  per               NUMERIC,      -- Price/Earnings ratio
  created_at        TIMESTAMPTZ   DEFAULT NOW(),

  UNIQUE (date, ticker)
);

-- Index for fast date-range queries
CREATE INDEX IF NOT EXISTS idx_cotation_journaliere_ticker_date
  ON brvm_cotation_journaliere (ticker, date DESC);

CREATE INDEX IF NOT EXISTS idx_cotation_journaliere_date
  ON brvm_cotation_journaliere (date DESC);

-- Enable Row Level Security (read-only for anon)
ALTER TABLE brvm_cotation_journaliere ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public read access"
  ON brvm_cotation_journaliere
  FOR SELECT
  TO anon
  USING (true);
