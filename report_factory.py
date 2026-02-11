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
    get_output_folder,
    save_generation_log,
    generate_pdf,
    Timer,
    FONT_REGULAR,
    FONT_BOLD,
    TEXT_COLOR,
    BASE_OUTPUT_DIR,
)

from report_types import ReportType, TableConfig, ReportDataV2


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
        has_nr_filter = '{nr}' in q or re.search(r"nr_umowy\s*=\s*'[^']*'", q)
        has_lok_filter = '{lok}' in q or re.search(r"nazwa_sklepu\s*=\s*'[^']*'", q)

        # Usuń filtr nr_umowy z WHERE
        q_clean = re.sub(
            r"\s+AND\s+nr_umowy\s*=\s*'[^']*'",
            '', q, flags=re.IGNORECASE
        )
        q_clean = re.sub(
            r"nr_umowy\s*=\s*'[^']*'\s*AND\s*",
            '', q_clean, flags=re.IGNORECASE
        )
        q_clean = re.sub(
            r"nr_umowy\s*=\s*'[^']*'",
            '1=1', q_clean, flags=re.IGNORECASE
        )

        # Usuń filtr nazwa_sklepu z WHERE
        q_clean = re.sub(
            r"\s+AND\s+nazwa_sklepu\s*=\s*'[^']*'",
            '', q_clean, flags=re.IGNORECASE
        )
        q_clean = re.sub(
            r"nazwa_sklepu\s*=\s*'[^']*'\s*AND\s*",
            '', q_clean, flags=re.IGNORECASE
        )
        q_clean = re.sub(
            r"nazwa_sklepu\s*=\s*'[^']*'",
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
            # Fallback – zapytanie tradycyjne
            resolved = query_template.replace('{nr}', str(nr)).strip()
            if lokalizacja:
                resolved = resolved.replace('{lok}', str(lokalizacja))
            cursor = self._conn.cursor()
            try:
                cursor.execute(resolved)
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

        # Fallback – bezpośrednie zapytanie (bez batch)
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            resolved_query = query.replace('{nr}', str(nr)).strip()
            if lokalizacja:
                resolved_query = resolved_query.replace('{lok}', str(lokalizacja))
            print(f"    SQL: {resolved_query}")
            cursor.execute(resolved_query)
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

        # Pierwszy wiersz wyników → mapuj na etykiety
        values = rows[0]
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
        Zwraca listę słowników: [{'nr_umowy': ..., 'klient': ..., 'podtyp_klient': ...}, ...]
        
        DOSTOSUJ zapytanie do swojej tabeli!
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # ZMIEŃ nazwę tabeli na swoją:
            query = """
                SELECT DISTINCT nr_umowy, klient, podtyp_klient 
                FROM your_contracts_table
                ORDER BY nr_umowy
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            contracts = []
            for row in rows:
                if row and len(row) >= 3:
                    contracts.append({
                        'nr_umowy': str(row[0]),
                        'klient': str(row[1]),
                        'podtyp_klient': str(row[2]),
                    })
            return contracts
        finally:
            cursor.close()

    def get_clients_list(self) -> List[str]:
        """
        Zwraca listę unikalnych nr_umowy (kompatybilność wsteczna).
        """
        contracts = self.get_contracts_list()
        return [c['nr_umowy'] for c in contracts]

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

        # Fallback – bezpośrednie zapytanie
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            query = f"""
                SELECT DISTINCT nazwa_sklepu 
                FROM your_shops_table
                WHERE nr_umowy = '{nr_umowy}'
                ORDER BY nazwa_sklepu
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            return [str(row[0]) for row in rows if row]
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    #  GENEROWANIE RAPORTÓW
    # ------------------------------------------------------------------

    def generate_report(self, report_type: ReportType, nr: str,
                        lokalizacja: str = None) -> str:
        """
        Generuje pojedynczy raport PDF dla danego numeru umowy.

        Args:
            report_type: Typ raportu (enum)
            nr: Numer umowy (parametr do zapytania SQL)
            lokalizacja: Nazwa lokalizacji (tylko dla Typ B)

        Returns:
            Ścieżka do wygenerowanego PDF
        """
        if report_type not in self.configs:
            raise ValueError(f"Brak konfiguracji: {report_type.value}")

        config = self.configs[report_type]
        print(f"\n→ {config['report_type']} | NR={nr}")

        # Pobierz podsumowanie
        with Timer("  Podsumowanie"):
            summary_data = self._fetch_summary(config, nr)

        # Nazwa klienta z pierwszego wiersza podsumowania (jeśli jest)
        client_name = summary_data[0][1] if summary_data and len(summary_data[0]) > 1 else str(nr)

        # Pobierz dane tabel
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
                    'percentage_columns': fmt.get('percentage_columns', []),
                    'add_total_row': fmt.get('add_total_row', True),
                    'column_widths': fmt.get('column_widths'),
                })

        # Generuj PDF — każdy NR = osobny plik
        safe_nr = str(nr).replace('/', '_').replace('\\', '_')
        if lokalizacja:
            safe_lok = lokalizacja.replace(' ', '_').replace('/', '_')
            filename = f"{report_type.value}_{safe_nr}_{safe_lok}.pdf"
        else:
            filename = f"{report_type.value}_{safe_nr}.pdf"
        subfolder = report_type.value.replace('_', '#')

        with Timer(f"  PDF ({len(tables)} tabel)"):
            pdf_path = generate_pdf(
                client_name=client_name,
                summary_title=config['summary']['title'],
                summary_data=summary_data,
                tables=tables,
                filename=filename,
                subfolder=subfolder,
            )

        return pdf_path

    def generate_all_for_nr(self, nr: str,
                            report_types: List[ReportType] = None) -> List[str]:
        """
        Generuje wybrane (lub wszystkie) typy raportów dla numeru umowy.

        Typ A = generuje dla każdej umowy (wszystkie sklepy)
        Typ B = generuje dla każdej lokalizacji osobno

        Args:
            nr: Numer umowy
            report_types: Lista typów (None = wszystkie skonfigurowane)

        Returns:
            Lista ścieżek do wygenerowanych PDF
        """
        types = report_types or list(self.configs.keys())
        generated = []

        for rt in types:
            if rt not in self.configs:
                print(f"  ⚠️  Pomijam {rt.value} - brak konfiguracji")
                continue
            try:
                # Typ B – generuj per lokalizacja (osobny raport dla każdego sklepu)
                if rt in (ReportType.WYKONANIE_A_TYP_B_TYDZIEN,
                          ReportType.WYKONANIE_B_TYP_B_TYDZIEN):
                    locations = self.get_locations_for_contract(nr)
                    print(f"  📍 Lokalizacje ({len(locations)}): {locations}")
                    for lok in locations:
                        try:
                            path = self.generate_report(rt, nr, lokalizacja=lok)
                            generated.append(path)
                        except Exception as e:
                            print(f"  ✗ {rt.value} lok={lok}: {e}")
                # Typ A – jeden raport per umowa (wszystkie sklepy razem)
                elif rt in (ReportType.WYKONANIE_A_TYP_A_TYDZIEN,
                            ReportType.WYKONANIE_B_TYP_A_TYDZIEN,
                            ReportType.ROZLICZENIE_A_KWARTAL,
                            ReportType.ROZLICZENIE_B_KWARTAL,
                            ReportType.ROZLICZENIE_B_MIESIAC):
                    path = self.generate_report(rt, nr)
                    generated.append(path)
                else:
                    # Fallback dla nieznanych typów
                    path = self.generate_report(rt, nr)
                    generated.append(path)
            except Exception as e:
                print(f"  ✗ {rt.value}: {e}")

        return generated

