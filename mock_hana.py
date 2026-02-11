# -*- coding: utf-8 -*-
"""
Mock SAP HANA – symuluje hdbcli.dbapi.connect().
Zwraca realistyczne dane dla wszystkich zapytań używanych w raportach.

Aby przełączyć na prawdziwą bazę:
  1. W main.py ustaw USE_MOCK = False
  2. Uzupełnij HANA_CONFIG danymi połączeniowymi
  3. Upewnij się, że hdbcli jest zainstalowane
"""

import random
import string
from typing import List, Any, Optional


# =========================================================================
#  DANE MOCKOWE
# =========================================================================

# Lista umów / klientów  (SELECT DISTINCT nr_umowy, klient, podtyp_klient)
MOCK_CONTRACTS = [
    {"nr_umowy": "UM-2026-001", "klient": "ABC Sp. z o.o.",        "podtyp_klient": "A"},
    {"nr_umowy": "UM-2026-002", "klient": "XYZ S.A.",              "podtyp_klient": "A"},
    {"nr_umowy": "UM-2026-003", "klient": "Handel Plus Sp. z o.o.","podtyp_klient": "B"},
    {"nr_umowy": "UM-2026-004", "klient": "MarketPro S.A.",        "podtyp_klient": "A"},
    {"nr_umowy": "UM-2026-005", "klient": "SuperShop Sp. z o.o.",  "podtyp_klient": "B"},
]

# Sklepy pogrupowane wg grupa_umowa (x, y, z)
MOCK_SHOPS = {
    "x": [
        {"nazwa_sklepu": "Sklep Centrum",     "klasyfikacja": "Premium", "typ_lok": "Galeria handlowa"},
        {"nazwa_sklepu": "Sklep Północ",      "klasyfikacja": "Standard","typ_lok": "Wolnostojący"},
        {"nazwa_sklepu": "Sklep Wschód",       "klasyfikacja": "Premium", "typ_lok": "Galeria handlowa"},
        {"nazwa_sklepu": "Sklep Zachód",       "klasyfikacja": "Standard","typ_lok": "Park handlowy"},
    ],
    "y": [
        {"nazwa_sklepu": "Market Południe",   "klasyfikacja": "Premium", "typ_lok": "Galeria handlowa"},
        {"nazwa_sklepu": "Market Zachód",      "klasyfikacja": "Economy", "typ_lok": "Wolnostojący"},
        {"nazwa_sklepu": "Market Centrum",    "klasyfikacja": "Standard","typ_lok": "Park handlowy"},
    ],
    "z": [
        {"nazwa_sklepu": "Hipermarket A",     "klasyfikacja": "Premium", "typ_lok": "Wolnostojący"},
        {"nazwa_sklepu": "Hipermarket B",     "klasyfikacja": "Standard","typ_lok": "Park handlowy"},
        {"nazwa_sklepu": "Hipermarket C",     "klasyfikacja": "Economy", "typ_lok": "Galeria handlowa"},
        {"nazwa_sklepu": "Hipermarket D",     "klasyfikacja": "Premium", "typ_lok": "Wolnostojący"},
        {"nazwa_sklepu": "Hipermarket E",     "klasyfikacja": "Standard","typ_lok": "Park handlowy"},
    ],
}


def _rand_currency(lo: float = 1000, hi: float = 50000) -> float:
    """Losowa kwota PLN."""
    return round(random.uniform(lo, hi), 2)


def _rand_pct(lo: float = 0.5, hi: float = 15.0) -> float:
    """Losowy procent."""
    return round(random.uniform(lo, hi), 2)


def _generate_summary_row(nr_umowy: str) -> list:
    """Generuje wiersz podsumowania (Tabela 1 – 8 kolumn)."""
    contract = None
    for c in MOCK_CONTRACTS:
        if c["nr_umowy"] == nr_umowy:
            contract = c
            break
    if contract is None:
        contract = MOCK_CONTRACTS[0]

    return [
        nr_umowy,                           # Nr umowy
        contract["podtyp_klient"],          # Typ
        contract["klient"],                 # Nazwa
        "Warszawa",                         # Lok
        "2026",                             # Rok
        "Q1",                               # Kwar
        "01.01.2026 - 31.03.2026",          # DR (data rozliczenia)
        "raport@firma.pl",                  # EMail
    ]


def _generate_header_table_row(nr_umowy: str) -> list:
    """Generuje wiersz Tabeli 2 – nagłówek duży (5 kolumn)."""
    rabat1 = _rand_pct(1.0, 10.0)
    rabat2 = _rand_pct(0.5, 8.0)
    udzielony = _rand_currency(5000, 80000)
    wartosc = _rand_currency(10000, 150000)
    wyplata = _rand_currency(2000, 50000)
    return [rabat1, rabat2, udzielony, wartosc, wyplata]


def _generate_shop_rows(grupa_umowa: str, nr_umowy: str) -> List[list]:
    """
    Generuje wiersze Tabeli 3 dla danej grupy umowy.
    Kolumny: nazwa_sklepu, grupa_umowa, klasyfikacja, typ_lok,
             podstawa, bonus%, wyprac%, bonus_lacz%, wartosc_bonus,
             udzielony_rabat, wartosc_do_wyr
    """
    shops = MOCK_SHOPS.get(grupa_umowa, [])
    rows = []
    for shop in shops:
        podstawa = _rand_currency(5000, 120000)
        bonus = _rand_pct(1.0, 12.0)
        wyprac = _rand_pct(0.5, 10.0)
        bonus_lacz = round(bonus + wyprac, 2)
        wartosc_bonus = round(podstawa * bonus / 100, 2)
        udzielony_rabat = round(podstawa * _rand_pct(0.1, 3.0) / 100, 2)
        wartosc_do_wyr = round(wartosc_bonus - udzielony_rabat, 2)

        rows.append([
            shop["nazwa_sklepu"],
            grupa_umowa,
            shop["klasyfikacja"],
            shop["typ_lok"],
            podstawa,
            bonus,
            wyprac,
            bonus_lacz,
            wartosc_bonus,
            udzielony_rabat,
            wartosc_do_wyr,
        ])
    return rows


def _generate_shop_rows_single(grupa_umowa: str, nr_umowy: str, lokalizacja: str) -> List[list]:
    """
    Generuje wiersze Tabeli 3 ale tylko dla wybranej lokalizacji (Typ B).
    """
    all_rows = _generate_shop_rows(grupa_umowa, nr_umowy)
    # Filtruj do wybranej lokalizacji; jeśli nie znaleziono – zwróć pierwszy
    filtered = [r for r in all_rows if r[0] == lokalizacja]
    if not filtered and all_rows:
        filtered = [all_rows[0]]
    return filtered


# =========================================================================
#  MAPOWANIE ZAPYTAŃ → DANYCH
# =========================================================================

def _resolve_query(query: str) -> List[list]:
    """
    Rozpoznaje zapytanie SQL / CALL i zwraca odpowiednie dane mockowe.
    Obsługiwane wzorce:
      - SELECT DISTINCT nr_umowy, klient, podtyp_klient ...
      - CALL SCHEMA.PROC_SUMMARY_WYKONANIE_A({nr})
      - CALL SCHEMA.PROC_HEADER_WYKONANIE_A({nr})
      - CALL SCHEMA.PROC_SKLEPY_WYKONANIE_A_{x|y|z}({nr})
      - CALL SCHEMA.PROC_SKLEPY_WYKONANIE_A_{x|y|z}_LOK({nr}, '{lok}')
    """
    q = query.strip().upper()

    # --- Lista umów ---
    if "SELECT" in q and "NR_UMOWY" in q and "KLIENT" in q:
        return [
            [c["nr_umowy"], c["klient"], c["podtyp_klient"]]
            for c in MOCK_CONTRACTS
        ]

    # --- Podsumowanie (Tabela 1) ---
    if "PROC_SUMMARY_WYKONANIE" in q:
        # wyciągnij nr_umowy z parametru
        nr = _extract_param(query)
        return [_generate_summary_row(nr)]

    # --- Nagłówek duży (Tabela 2) ---
    if "PROC_HEADER_WYKONANIE" in q:
        nr = _extract_param(query)
        return [_generate_header_table_row(nr)]

    # --- Sklepy – pojedyncza lokalizacja (Typ B) ---
    if "PROC_SKLEPY_WYKONANIE" in q and "_LOK" in q:
        nr = _extract_first_param(query)
        lok = _extract_second_param(query)
        grupa = _extract_grupa(query)
        return _generate_shop_rows_single(grupa, nr, lok)

    # --- Sklepy – wszystkie (Typ A) ---
    if "PROC_SKLEPY_WYKONANIE" in q:
        nr = _extract_param(query)
        grupa = _extract_grupa(query)
        return _generate_shop_rows(grupa, nr)

    # --- Fallback ---
    print(f"  ⚠️  Mock: nierozpoznane zapytanie: {query}")
    return []


def _extract_param(query: str) -> str:
    """Wyciąga parametr z CALL PROC(param)."""
    try:
        start = query.index("(") + 1
        end = query.index(")")
        return query[start:end].strip().strip("'\"")
    except ValueError:
        return ""


def _extract_first_param(query: str) -> str:
    """Wyciąga pierwszy parametr z CALL PROC(p1, p2)."""
    try:
        start = query.index("(") + 1
        end = query.index(")")
        params = query[start:end].split(",")
        return params[0].strip().strip("'\"")
    except (ValueError, IndexError):
        return ""


def _extract_second_param(query: str) -> str:
    """Wyciąga drugi parametr z CALL PROC(p1, p2)."""
    try:
        start = query.index("(") + 1
        end = query.index(")")
        params = query[start:end].split(",")
        return params[1].strip().strip("'\"") if len(params) > 1 else ""
    except (ValueError, IndexError):
        return ""


def _extract_grupa(query: str) -> str:
    """Wyciąga grupę umowy (x/y/z) z nazwy procedury."""
    q = query.upper()
    for g in ["_Z(", "_Z_", "_Y(", "_Y_", "_X(", "_X_"]:
        if g in q:
            return g[1].lower()
    return "x"


# =========================================================================
#  MOCK KLAS hdbcli.dbapi
# =========================================================================

class MockCursor:
    """Symuluje hdbcli cursor."""

    def __init__(self):
        self._results: List[list] = []
        self._description = None

    def execute(self, query: str):
        self._results = _resolve_query(query)

    def fetchall(self) -> List[list]:
        return self._results

    def fetchone(self) -> Optional[list]:
        return self._results[0] if self._results else None

    def close(self):
        pass


class MockConnection:
    """Symuluje hdbcli connection."""

    def __init__(self, **kwargs):
        self._config = kwargs

    def cursor(self) -> MockCursor:
        return MockCursor()

    def close(self):
        pass


class MockDbApi:
    """Symuluje moduł hdbcli.dbapi."""

    @staticmethod
    def connect(**kwargs) -> MockConnection:
        print("  🔌 Mock HANA: połączono (symulacja)")
        return MockConnection(**kwargs)


# Singleton – importuj i używaj jak prawdziwego dbapi
mock_dbapi = MockDbApi()
