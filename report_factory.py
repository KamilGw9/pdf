# -*- coding: utf-8 -*-
"""
ReportFactory - ładuje konfigurację z YAML i generuje raporty PDF.
Obsługuje zapytania SELECT jako źródło danych.
Jeden numer = jeden PDF. Zmiana numeru = nowy plik PDF.

OPTYMALIZACJE:
  1. Jedno połączenie na cały run (connection pooling)
  2. Batch query – każde zapytanie wykonywane RAZ (bez WHERE nr_umowy),
     wyniki cache'owane i filtrowane w Pythonie
  3. Cache summary/header – identyczne zapytania nie powtarzają się
"""

import re
import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import os

from main import (
    get_hana_connection,
    Timer,
)

from report_types import ReportType


# ======================================================================
#  PARAMETRYZACJA ZAPYTAŃ SQL
# ======================================================================

def _parameterize_query(query: str, nr: str = None,
                        lokalizacja: str = None) -> tuple:
    """
    Zamienia placeholdery {nr}/{lok} na parametry wiązane (?).
    Zwraca (query_z_?, lista_parametrów) — bezpieczne przed SQL injection.
    """
    params = []
    result = query

    # Zbierz pozycje placeholderów i posortuj wg pozycji
    replacements = []
    if nr is not None and "'{nr}'" in result:
        replacements.append(("'{nr}'", str(nr), result.find("'{nr}'")))
    if lokalizacja is not None and "'{lok}'" in result:
        replacements.append(("'{lok}'", str(lokalizacja), result.find("'{lok}'")))

    # Sortuj wg pozycji w query (zachowaj kolejność parametrów)
    replacements.sort(key=lambda x: x[2])

    for placeholder, value, _ in replacements:
        result = result.replace(placeholder, '?', 1)
        params.append(value)

    return result.strip(), params


# ======================================================================
#  BATCH QUERY ENGINE
# ======================================================================

class QueryCache:
    """
    Cache zapytań – wykonuje każde zapytanie RAZ na cały zbiór danych,
    potem filtruje w Pythonie po nr_umowy / lokalizacja.

    Zamiast:  1000 umów × 5 tabel = 5000 zapytań
    Teraz:    5 unikalnych zapytań + filtrowanie w pamięci
    """

    # Kolumna wg której filtrujemy nr_umowy (indeks w wynikach)
    NR_UMOWY_FILTER_COL = 'nr_umowy'

    def __init__(self, connection):
        self._conn = connection
        self._cache: Dict[str, List[list]] = {}  # base_query → all rows
        self._base_query_cache: Dict[str, tuple] = {}  # template → (base_q, nr_col, lok_col, added)
        self._query_count = 0

    @property
    def query_count(self) -> int:
        return self._query_count

    def _make_base_query(self, query: str) -> Tuple[str, Optional[int], Optional[int]]:
        """
        Przekształca zapytanie per-umowa na zapytanie batch (bez filtrów).
        Zwraca: (base_query, indeks_kolumny_nr, indeks_kolumny_lok)

        Przykład:
          IN:  SELECT a, b, c FROM tab WHERE nr_umowy = 'UM-001' AND grupa = 'X'
          OUT: SELECT nr_umowy, nazwa_sklepu, a, b, c FROM tab WHERE grupa = 'X'
               nr_col=0, lok_col=1

        Strategia: usuwamy WHERE nr_umowy = '{nr}' (i AND nazwa_sklepu = '{lok}'),
        ale dodajemy te kolumny do SELECT żeby móc filtrować w Pythonie.
        """
        q = query.strip()

        # Wykryj oryginalne filtry (przed podstawieniem {nr}/{lok})
        # Obsługuje zarówno placeholdery {nr}/{lok} jak i literały 'wartość'
        _nr_pattern = r"nr_umowy\s*=\s*(?:'\{nr\}'|'[^']*')"
        _lok_pattern = r"nazwa_sklepu\s*=\s*(?:'\{lok\}'|'[^']*')"
        has_nr_filter = bool(re.search(_nr_pattern, q, re.IGNORECASE))
        has_lok_filter = bool(re.search(_lok_pattern, q, re.IGNORECASE))

        # Usuń filtr nr_umowy z WHERE (trzy warianty pozycji w klauzuli)
        q_clean = re.sub(
            r"\s+AND\s+" + _nr_pattern,
            '', q, flags=re.IGNORECASE
        )
        q_clean = re.sub(
            _nr_pattern + r"\s*AND\s*",
            '', q_clean, flags=re.IGNORECASE
        )
        q_clean = re.sub(
            _nr_pattern,
            '1=1', q_clean, flags=re.IGNORECASE
        )

        # Usuń filtr nazwa_sklepu z WHERE
        q_clean = re.sub(
            r"\s+AND\s+" + _lok_pattern,
            '', q_clean, flags=re.IGNORECASE
        )
        q_clean = re.sub(
            _lok_pattern + r"\s*AND\s*",
            '', q_clean, flags=re.IGNORECASE
        )
        q_clean = re.sub(
            _lok_pattern,
            '1=1', q_clean, flags=re.IGNORECASE
        )

        # Wyczyść "WHERE 1=1" jeśli nic innego nie zostało
        q_clean = re.sub(r"WHERE\s+1=1\s*$", '', q_clean, flags=re.IGNORECASE)
        q_clean = re.sub(r"WHERE\s+1=1\s+AND\s+", 'WHERE ', q_clean, flags=re.IGNORECASE)

        # Dodaj kolumny filtrujące na początek SELECT
        # (tylko jeśli nie ma ich już w SELECT)
        # Obsługa SELECT DISTINCT – zachowaj DISTINCT po dodaniu kolumn
        has_distinct = bool(re.match(r'\s*SELECT\s+DISTINCT\s+', q_clean, re.IGNORECASE))
        select_match = re.search(
            r'SELECT\s+(?:DISTINCT\s+)?(.+?)\s+FROM',
            q_clean, re.IGNORECASE | re.DOTALL
        )
        existing_cols = select_match.group(1).lower() if select_match else ''

        extra_cols = []
        if has_nr_filter and 'nr_umowy' not in existing_cols:
            extra_cols.append('nr_umowy')
        if has_lok_filter and 'nazwa_sklepu' not in existing_cols:
            extra_cols.append('nazwa_sklepu')

        if extra_cols:
            prefix = ', '.join(extra_cols) + ', '
            if has_distinct:
                # SELECT DISTINCT col1 → SELECT DISTINCT nr_umowy, col1
                q_clean = re.sub(
                    r'SELECT\s+DISTINCT\s+',
                    f'SELECT DISTINCT {prefix}',
                    q_clean,
                    count=1,
                    flags=re.IGNORECASE,
                )
            else:
                q_clean = re.sub(
                    r'SELECT\s+',
                    f'SELECT {prefix}',
                    q_clean,
                    count=1,
                    flags=re.IGNORECASE,
                )

        # Ustal indeksy kolumn filtrujących w wyniku zapytania batch
        nr_col = None
        lok_col = None
        added_count = len(extra_cols)

        if has_nr_filter:
            if 'nr_umowy' in [c.lower() for c in extra_cols]:
                nr_col = extra_cols.index('nr_umowy')
            else:
                # nr_umowy jest już w oryginalnym SELECT – znajdź pozycję
                cols = [c.strip().lower() for c in existing_cols.split(',')]
                nr_col = added_count + cols.index('nr_umowy') if 'nr_umowy' in cols else None

        if has_lok_filter:
            if 'nazwa_sklepu' in [c.lower() for c in extra_cols]:
                lok_col = extra_cols.index('nazwa_sklepu')
            else:
                cols = [c.strip().lower() for c in existing_cols.split(',')]
                lok_col = added_count + cols.index('nazwa_sklepu') if 'nazwa_sklepu' in cols else None

        return q_clean.strip(), nr_col, lok_col, added_count

    def _get_template_query(self, query: str) -> str:
        """Zwraca query z placeholder'ami {nr}/{lok} — jako klucz cache."""
        return query.strip()

    def _get_base_query_cached(self, template: str) -> tuple:
        """Cache'owana wersja _make_base_query — regex tylko raz."""
        if template not in self._base_query_cache:
            self._base_query_cache[template] = self._make_base_query(template)
        return self._base_query_cache[template]

    def prefetch(self, queries: List[str]):
        """
        Pobiera dane dla wszystkich unikalnych zapytań naraz.
        Wywoływane RAZ przed generowaniem raportów.
        """
        seen = set()
        for query_template in queries:
            template = self._get_template_query(query_template)
            if template in seen or template in self._cache:
                continue
            seen.add(template)

            base_q, nr_col, lok_col, added = self._get_base_query_cached(template)

            cursor = self._conn.cursor()
            try:
                print(f"    📦 Batch: {base_q[:120]}...")
                cursor.execute(base_q)
                rows = cursor.fetchall()
                self._cache[template] = [
                    [str(cell) if cell is not None else '' for cell in row]
                    for row in rows
                ]
                self._query_count += 1
                print(f"       → {len(rows)} wierszy")
            except Exception as e:
                print(f"    ✗ Batch error: {e}")
                self._cache[template] = []
            finally:
                cursor.close()

    def get(self, query_template: str, nr: str = None,
            lokalizacja: str = None) -> List[List[str]]:
        """
        Zwraca przefiltrowane wyniki z cache.
        Jeśli nie ma w cache, wykonuje zapytanie tradycyjnie (fallback).
        """
        template = self._get_template_query(query_template)
        _, nr_col, lok_col, added_count = self._get_base_query_cached(template)

        if template not in self._cache:
            # Fallback – zapytanie z parametrami wiązanymi (bezpieczne)
            resolved, params = _parameterize_query(
                query_template, nr=nr, lokalizacja=lokalizacja
            )
            cursor = self._conn.cursor()
            try:
                cursor.execute(resolved, params)
                rows = cursor.fetchall()
                self._query_count += 1
                return [
                    [str(c) if c is not None else '' for c in row]
                    for row in rows
                ]
            finally:
                cursor.close()

        all_rows = self._cache[template]

        # Filtruj po nr_umowy
        if nr is not None and nr_col is not None:
            all_rows = [r for r in all_rows if r[nr_col] == str(nr)]

        # Filtruj po lokalizacja
        if lokalizacja is not None and lok_col is not None:
            all_rows = [r for r in all_rows if r[lok_col] == str(lokalizacja)]

        # Usuń tylko kolumny które DODALIŚMY na początek (extra_cols)
        if added_count > 0:
            return [row[added_count:] for row in all_rows]

        return all_rows


class ReportFactory:
    """
    Factory do generowania raportów z dynamiczną liczbą tabel.
    Konfiguracja z plików YAML, dane z SAP HANA (SELECT).
    
    Optymalizacje:
      - Jedno połączenie na cały cykl generowania
      - Batch prefetch – 1 zapytanie zamiast N
      - Cache wyników
    """

    def __init__(self, config_dir: str = "config", connection=None):
        self.config_dir = Path(config_dir)
        self.configs: Dict[ReportType, dict] = {}
        self._connection = connection  # jedno połączenie na cały run
        self._owns_connection = connection is None
        self._cache: Optional[QueryCache] = None
        self._load_all_configs()

    def _get_connection(self):
        """Zwraca połączenie – tworzy jeśli nie przekazano."""
        if self._connection is None:
            self._connection = get_hana_connection()
            self._owns_connection = True
        return self._connection

    def close(self):
        """Zamyka połączenie jeśli factory je utworzyło."""
        if self._owns_connection and self._connection is not None:
            self._connection.close()
            self._connection = None

    def _load_all_configs(self):
        """Ładuje wszystkie konfiguracje YAML."""
        print(f"\n📂 Konfiguracje: {self.config_dir.resolve()}")
        for report_type in ReportType:
            config_path = self.config_dir / f"{report_type.value}.yaml"
            if not config_path.exists():
                print(f"  ⚠️  Brak: {config_path.name}")
                continue
            with open(config_path, 'r', encoding='utf-8') as f:
                self.configs[report_type] = yaml.safe_load(f)
            tables = len(self.configs[report_type].get('data_tables', []))
            print(f"  ✓ {report_type.value} ({tables} tabel)")

    # ------------------------------------------------------------------
    #  BATCH PREFETCH
    # ------------------------------------------------------------------

    def prepare_batch(self, report_types: List[ReportType] = None):
        """
        Prefetch: pobiera WSZYSTKIE dane za jednym razem.
        Wywołaj RAZ przed pętlą po umowach.

        Zamiast 1000 umów × 5 tabel = 5000 zapytań
        → max ~5 zapytań (1 per unikalną tabelę).
        """
        conn = self._get_connection()
        self._cache = QueryCache(conn)

        types = report_types or list(self.configs.keys())
        all_queries = []

        for rt in types:
            if rt not in self.configs:
                continue
            cfg = self.configs[rt]
            # summary query
            all_queries.append(cfg['summary']['query'])
            # data table queries
            for table_cfg in cfg['data_tables']:
                all_queries.append(table_cfg['query'])

        print(f"\n🚀 BATCH PREFETCH: {len(all_queries)} zapytań do pobrania")
        self._cache.prefetch(all_queries)
        print(f"   ✓ Wykonano {self._cache.query_count} unikalnych zapytań SQL")

    # ------------------------------------------------------------------
    #  POBIERANIE DANYCH Z HANA (z cache lub direct)
    # ------------------------------------------------------------------

    def _execute_query(self, query: str, nr: str, lokalizacja: str = None) -> List[List[str]]:
        """
        Pobiera dane – z cache (jeśli batch) lub bezpośrednio.
        """
        if self._cache is not None:
            return self._cache.get(query, nr=nr, lokalizacja=lokalizacja)

        # Fallback – bezpośrednie zapytanie z parametrami wiązanymi
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            resolved_query, params = _parameterize_query(
                query, nr=nr, lokalizacja=lokalizacja
            )
            print(f"    SQL: {resolved_query}  params={params}")
            cursor.execute(resolved_query, params)
            try:
                rows = cursor.fetchall()
                return [
                    [str(cell) if cell is not None else '' for cell in row]
                    for row in rows
                ]
            except Exception:
                return []
        finally:
            cursor.close()

    def _fetch_summary(self, config: dict, nr: str) -> List[List[str]]:
        """Pobiera dane podsumowania (pary etykieta/wartość)."""
        query = config['summary']['query']
        labels = config['summary']['labels']
        rows = self._execute_query(query, nr)

        if not rows:
            return [[label, ''] for label in labels]

        # Walidacja: liczba kolumn vs etykiet
        values = rows[0]
        if len(values) != len(labels):
            print(f"  ⚠️  Summary: oczekiwano {len(labels)} kolumn, "
                  f"otrzymano {len(values)} – dane mogą być niepoprawne")

        # Pierwszy wiersz wyników → mapuj na etykiety
        return [
            [label, values[i] if i < len(values) else '']
            for i, label in enumerate(labels)
        ]

    def _fetch_table_data(self, table_cfg: dict, nr: str,
                          lokalizacja: str = None) -> List[List[str]]:
        """Pobiera dane dla pojedynczej tabeli."""
        return self._execute_query(table_cfg['query'], nr, lokalizacja)

    # ------------------------------------------------------------------
    #  POBIERANIE LISTY UMÓW / KLIENTÓW
    # ------------------------------------------------------------------

    def get_contracts_list(self) -> List[Dict[str, Any]]:
        """
        Pobiera listę unikalnych umów z HANA.
        Zwraca listę słowników z kluczami:
          nr_umowy, klient, podtyp_klient, id, pla
        
        DOSTOSUJ zapytanie do swojej tabeli!
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # ZMIEŃ nazwę tabeli na swoją:
            query = """
                SELECT DISTINCT nr_umowy, klient, podtyp_klient, id, pla
                FROM your_contracts_table
                ORDER BY nr_umowy
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            contracts = []
            for row in rows:
                if row and len(row) >= 5:
                    contracts.append({
                        'nr_umowy': str(row[0]),
                        'klient': str(row[1]),
                        'podtyp_klient': str(row[2]),
                        'id': str(row[3]),
                        'pla': str(row[4]),
                    })
            return contracts
        finally:
            cursor.close()

    def get_locations_for_contract(self, nr_umowy: str) -> List[str]:
        """
        Pobiera listę lokalizacji (sklepów) dla danej umowy.
        Używane przy Typ B – raport per lokalizacja.
        Korzysta z cache jeśli dostępny (batch mode).
        
        DOSTOSUJ zapytanie do swojej tabeli!
        """
        # Próbuj z cache (batch) – szukaj zapytania sklepowego
        if self._cache is not None:
            for rt, cfg in self.configs.items():
                for table_cfg in cfg.get('data_tables', []):
                    q = table_cfg.get('query', '')
                    if 'nazwa_sklepu' in q.lower():
                        rows = self._cache.get(q, nr=nr_umowy)
                        # Kolumna 0 = nazwa_sklepu (pierwsza w SELECT)
                        shops = sorted(set(r[0] for r in rows if r))
                        if shops:
                            return shops

        # Fallback – bezpośrednie zapytanie z parametrem wiązanym
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            query = """
                SELECT DISTINCT nazwa_sklepu 
                FROM your_shops_table
                WHERE nr_umowy = ?
                ORDER BY nazwa_sklepu
            """
            cursor.execute(query, [str(nr_umowy)])
            rows = cursor.fetchall()
            return [str(row[0]) for row in rows if row]
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    #  GENEROWANIE RAPORTÓW
    # ------------------------------------------------------------------

    @staticmethod
    def _get_year_quarter() -> Tuple[str, str]:
        """Zwraca bieżący rok i kwartał, np. ('2026', 'Q1')."""
        now = datetime.now()
        quarter = (now.month - 1) // 3 + 1
        return str(now.year), f"Q{quarter}"

    def build_report_payload(self, report_type: ReportType, nr: str,
                             lokalizacja: str = None,
                             pla: str = None,
                             contract_id: str = None) -> Dict[str, Any]:
        """
        Buduje kompletny payload raportu (bez renderowania PDF).
        """
        if report_type not in self.configs:
            raise ValueError(f"Brak konfiguracji: {report_type.value}")

        config = self.configs[report_type]
        print(f"\n→ {config['report_type']} | NR={nr}")

        with Timer("  Podsumowanie"):
            summary_data = self._fetch_summary(config, nr)

        client_name = summary_data[0][1] if summary_data and len(summary_data[0]) > 1 else str(nr)

        tables = []
        for table_cfg in config['data_tables']:
            with Timer(f"  {table_cfg['title']}"):
                data = self._fetch_table_data(table_cfg, nr, lokalizacja)
                fmt = table_cfg.get('formatting', {})
                tables.append({
                    'title': table_cfg['title'],
                    'subtitle': table_cfg.get('subtitle', ''),
                    'headers': table_cfg['headers'],
                    'data': data,
                    'currency_columns': fmt.get('currency_columns', []),
                    'currency_columns_2': fmt.get('currency_columns_2', []),
                    'percentage_columns': fmt.get('percentage_columns', []),
                    'add_total_row': fmt.get('add_total_row', True),
                    'column_widths': fmt.get('column_widths'),
                })

        # --- Nazwa pliku PDF ---
        year, quarter = self._get_year_quarter()
        safe_pla = str(pla).replace('/', '_').replace('\\', '_') if pla else 'BRAK_PLA'
        safe_id = str(contract_id).replace('/', '_').replace('\\', '_') if contract_id else 'BRAK_ID'

        if lokalizacja:
            # Typ B: PLA_ID_2026_Q1_ZAMAWIAJACY.pdf
            filename = f"{safe_pla}_{safe_id}_{year}_{quarter}_ZAMAWIAJACY.pdf"
        else:
            # Typ A: PLA_ID_2026_Q1.pdf
            filename = f"{safe_pla}_{safe_id}_{year}_{quarter}.pdf"

        # Statystyki tabel (liczba wierszy danych)
        table_stats = [
            f"{t['title']}: {len(t['data'])} wierszy" for t in tables
        ]

        return {
            'client_name': client_name,
            'summary_title': config['summary']['title'],
            'summary_data': summary_data,
            'tables': tables,
            'filename': filename,
            'subfolder': report_type.value.replace('_', '#'),
            'table_stats': table_stats,
        }

