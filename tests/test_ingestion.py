"""Tests sans infrastructure : téléchargement SODA (HTTP mocké) et générateur synthétique."""
import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

from pipeline.ingestion import soda, synthetic


class FakeResponse:
    def __init__(self, text):
        self.content = text.encode("utf-8")

    def raise_for_status(self):
        pass


class FakeSession:
    """Simule l'API SODA : $limit/$offset sur un jeu de N lignes, en-tête répété à chaque page."""

    def __init__(self, n_rows):
        self.n_rows, self.calls = n_rows, []

    def get(self, url, params=None, timeout=None):
        self.calls.append(params)
        off, lim = params["$offset"], params["$limit"]
        rows = [f"id{i},2023-01-01T00:00:00.000" for i in range(off, min(off + lim, self.n_rows))]
        return FakeResponse("trip_id,trip_start_timestamp\n" + "".join(r + "\n" for r in rows))


class TestSoda(unittest.TestCase):
    def test_daterange_end_exclusive(self):
        days = list(soda.daterange(date(2023, 1, 1), date(2023, 1, 4)))
        self.assertEqual([d.day for d in days], [1, 2, 3])

    def test_build_where(self):
        w = soda.build_where(date(2023, 1, 31))
        self.assertIn("'2023-01-31T00:00:00'", w)
        self.assertIn("'2023-02-01T00:00:00'", w)

    def test_pagination_single_header_and_all_rows(self):
        sess = FakeSession(n_rows=25)
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "d" / "trips.csv"
            n = soda.download_day(date(2023, 1, 1), dest, page_size=10, session=sess)
            self.assertEqual(n, 25)
            self.assertEqual([c["$offset"] for c in sess.calls], [0, 10, 20])
            lines = dest.read_text().splitlines()
            self.assertEqual(lines[0], "trip_id,trip_start_timestamp")
            self.assertEqual(len(lines), 26)               # 1 en-tête + 25 lignes
            self.assertEqual(lines.count(lines[0]), 1)     # en-tête non répété

    def test_exact_multiple_of_page_size_stops_on_empty_page(self):
        sess = FakeSession(n_rows=20)
        with tempfile.TemporaryDirectory() as tmp:
            n = soda.download_day(date(2023, 1, 1), Path(tmp) / "t.csv", page_size=10, session=sess)
            self.assertEqual(n, 20)

    def test_max_rows_cap(self):
        sess = FakeSession(n_rows=1000)
        with tempfile.TemporaryDirectory() as tmp:
            n = soda.download_day(date(2023, 1, 1), Path(tmp) / "t.csv", page_size=10, max_rows=25, session=sess)
            self.assertEqual(n, 25)

    def test_empty_day_creates_no_file(self):
        sess = FakeSession(n_rows=0)
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "t.csv"
            self.assertEqual(soda.download_day(date(2023, 1, 1), dest, session=sess), 0)
            self.assertFalse(dest.exists())


class TestSynthetic(unittest.TestCase):
    def test_schema_and_anomalies(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "t.csv"
            n = synthetic.generate_day(date(2023, 1, 1), dest, n_rows=1000)
            with open(dest, newline="") as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), n)
            self.assertEqual(list(rows[0].keys()), soda.TRIP_COLUMNS)
            ids = [r["trip_id"] for r in rows if r["trip_id"]]
            self.assertLess(len(set(ids)), len(ids))                       # doublons présents
            self.assertTrue(any(r["trip_id"] == "" for r in rows))         # trip_id manquants
            self.assertTrue(any(float(r["fare"]) < 0 for r in rows))       # montants négatifs

    def test_deterministic(self):
        with tempfile.TemporaryDirectory() as tmp:
            a, b = Path(tmp) / "a.csv", Path(tmp) / "b.csv"
            synthetic.generate_day(date(2023, 1, 1), a, n_rows=200)
            synthetic.generate_day(date(2023, 1, 1), b, n_rows=200)
            self.assertEqual(a.read_text(), b.read_text())


if __name__ == "__main__":
    unittest.main()
